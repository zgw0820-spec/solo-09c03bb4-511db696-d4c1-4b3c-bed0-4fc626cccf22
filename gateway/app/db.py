"""SQLite persistence for the edge gateway.

Tables:
  upstreams  - registered upstream targets, each with a concurrency quota
               (capacity, 1..5). The quota is configuration and persists
               across restarts; occupancy never does.
  routes     - single-row table holding the effective route and its version
  inflight   - one row per admitted proxied request == one held quota slot;
               wiped on startup so a restarted gateway neither reports
               interrupted requests as in-flight nor leaks their quota slots
  probes     - audit log of candidate probes; a commit requires a fresh,
               successful probe recorded against the same route version

Requests that find the current target's quota exhausted are tracked as
*waiters* in process memory only (never in SQLite): a wait that times out
leaves no slot held, and a restart clears the waiters together with the
inflight table, so neither a timeout nor a restart leaves a placeholder.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

DB_PATH = Path(os.environ.get("GATEWAY_DB", "/data/gateway.db"))
PROBE_TTL_SECONDS = float(os.environ.get("PROBE_TTL_SECONDS", "60"))

MIN_CAPACITY = 1
MAX_CAPACITY = 5
DEFAULT_CAPACITY = max(
    MIN_CAPACITY, min(MAX_CAPACITY, int(os.environ.get("DEFAULT_CAPACITY", "3")))
)

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None
# wid -> {"id", "target", "path", "since"}; memory only, never persisted.
_waiters: dict[str, dict] = {}


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(upstreams: dict[str, str], initial_target: str) -> dict:
    """Create tables, upsert the upstream registry, seed the route once,
    and drop in-flight rows left behind by a previous (killed) process."""
    global _conn
    if not upstreams:
        raise RuntimeError("no upstreams configured")
    if initial_target not in upstreams:
        raise RuntimeError(f"initial target {initial_target!r} not in upstreams")
    _conn = _connect()
    with _lock, _conn:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS upstreams (
                name     TEXT PRIMARY KEY,
                url      TEXT NOT NULL,
                capacity INTEGER NOT NULL DEFAULT 3
            );
            CREATE TABLE IF NOT EXISTS routes (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                current_target TEXT NOT NULL,
                version        INTEGER NOT NULL,
                updated_at     REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inflight (
                id         TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                path       TEXT NOT NULL,
                started_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS probes (
                id           TEXT PRIMARY KEY,
                target       TEXT NOT NULL,
                base_version INTEGER NOT NULL,
                ok           INTEGER NOT NULL,
                detail       TEXT,
                created_at   REAL NOT NULL
            );
            """
        )
        # Databases created before quotas existed gain the column; existing
        # per-target quota settings are kept across restarts.
        cols = {r["name"] for r in _conn.execute("PRAGMA table_info(upstreams)")}
        if "capacity" not in cols:
            _conn.execute(
                "ALTER TABLE upstreams ADD COLUMN capacity "
                f"INTEGER NOT NULL DEFAULT {DEFAULT_CAPACITY}"
            )
        for name, url in upstreams.items():
            # Upsert the address only: an operator-set quota survives restarts.
            _conn.execute(
                "INSERT INTO upstreams (name, url, capacity) VALUES (?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET url = excluded.url",
                (name, url, DEFAULT_CAPACITY),
            )
        seeded = False
        if _conn.execute("SELECT COUNT(*) AS c FROM routes").fetchone()["c"] == 0:
            _conn.execute(
                "INSERT INTO routes (id, current_target, version, updated_at) "
                "VALUES (1, ?, 0, ?)",
                (initial_target, time.time()),
            )
            seeded = True
        # Interrupted requests of a previous process must not linger as
        # in-flight, and their quota slots must not stay occupied.
        cleared = _conn.execute("DELETE FROM inflight").rowcount
    return {"seeded_route": seeded, "cleared_inflight": cleared}


def _db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialised"
    return _conn


def get_route() -> dict:
    with _lock:
        row = _db().execute(
            "SELECT current_target, version, updated_at FROM routes WHERE id = 1"
        ).fetchone()
    return dict(row)


def upstream_url(name: str) -> str | None:
    with _lock:
        row = _db().execute(
            "SELECT url FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
    return row["url"] if row else None


def try_admit(waiter_id: str, path: str) -> dict:
    """One atomic admission attempt against the *current* route target.

    Route snapshot, quota check and in-flight registration happen in a
    single critical section, so admission, quota edits and route switches
    can never interleave into over-committing a target or pinning a
    request to a stale target. The request is registered in-flight (thus
    holding a quota slot) only after a slot of the current route target
    has actually been secured.

    If the target is full, the attempt is (re)recorded as waiting on that
    target — in memory only — so a retry after a route switch naturally
    competes for the new target's quota instead.
    """
    now = time.time()
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        target = route["current_target"]
        cap = conn.execute(
            "SELECT capacity FROM upstreams WHERE name = ?", (target,)
        ).fetchone()["capacity"]
        held = conn.execute(
            "SELECT COUNT(*) AS c FROM inflight WHERE target = ?", (target,)
        ).fetchone()["c"]
        if held < cap:
            rid = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO inflight (id, target, path, started_at) "
                "VALUES (?, ?, ?, ?)",
                (rid, target, path, now),
            )
            _waiters.pop(waiter_id, None)
            return {
                "admitted": True,
                "id": rid,
                "target": target,
                "version": route["version"],
                "capacity": cap,
            }
        prev = _waiters.get(waiter_id)
        _waiters[waiter_id] = {
            "id": waiter_id,
            "target": target,
            "path": path,
            "since": prev["since"] if prev else now,
        }
        return {
            "admitted": False,
            "target": target,
            "version": route["version"],
            "capacity": cap,
            "held": held,
        }


def waiter_done(waiter_id: str) -> None:
    """Drop a waiter record (admitted, timed out, or errored)."""
    with _lock:
        _waiters.pop(waiter_id, None)


def finish(request_id: str) -> None:
    with _lock, _db() as conn:
        conn.execute("DELETE FROM inflight WHERE id = ?", (request_id,))


def set_capacity(name: str, capacity: int) -> dict:
    """Update one target's quota (1..5). Lowering the quota never interrupts
    in-flight requests: occupancy may stay above the new quota until those
    requests finish, and no new admission happens until it drops back below.
    """
    if not MIN_CAPACITY <= capacity <= MAX_CAPACITY:
        return {
            "ok": False,
            "error": "capacity_out_of_range",
            "min": MIN_CAPACITY,
            "max": MAX_CAPACITY,
        }
    with _lock, _db() as conn:
        cur = conn.execute(
            "UPDATE upstreams SET capacity = ? WHERE name = ?", (capacity, name)
        )
        if cur.rowcount != 1:
            return {"ok": False, "error": "unknown_target"}
        held = conn.execute(
            "SELECT COUNT(*) AS c FROM inflight WHERE target = ?", (name,)
        ).fetchone()["c"]
    return {"ok": True, "target": name, "capacity": capacity, "inflight": held}


def record_probe(target: str, base_version: int, ok: bool, detail: dict) -> dict:
    """A probe never mutates the route; it only records an auditable result."""
    pid = uuid.uuid4().hex
    with _lock, _db() as conn:
        conn.execute(
            "INSERT INTO probes (id, target, base_version, ok, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pid, target, base_version, 1 if ok else 0, json.dumps(detail), time.time()),
        )
    return {"id": pid}


def commit_switch(target: str, base_version: int, probe_id: str) -> dict:
    """Compare-and-swap the route on its version. A commit carrying a stale
    version (e.g. a late probe from an interleaved switch attempt) loses and
    cannot overwrite the already-effective route."""
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        if route["version"] != base_version:
            return {
                "ok": False,
                "error": "version_conflict",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if route["current_target"] == target:
            return {
                "ok": False,
                "error": "target_is_current",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if upstream_url(target) is None:
            return {"ok": False, "error": "unknown_target"}
        probe = conn.execute(
            "SELECT * FROM probes WHERE id = ?", (probe_id,)
        ).fetchone()
        if (
            probe is None
            or not probe["ok"]
            or probe["target"] != target
            or probe["base_version"] != base_version
        ):
            return {"ok": False, "error": "probe_invalid"}
        if time.time() - probe["created_at"] > PROBE_TTL_SECONDS:
            return {"ok": False, "error": "probe_stale"}
        cur = conn.execute(
            "UPDATE routes SET current_target = ?, version = version + 1, updated_at = ? "
            "WHERE id = 1 AND version = ?",
            (target, time.time(), base_version),
        )
        if cur.rowcount != 1:  # guarded by _lock; belt and braces
            return {"ok": False, "error": "version_conflict"}
        return {"ok": True, "current_target": target, "version": base_version + 1}


def get_state() -> dict:
    with _lock:
        route = dict(_db().execute("SELECT * FROM routes WHERE id = 1").fetchone())
        upstreams = [
            dict(r)
            for r in _db().execute(
                "SELECT name, url, capacity FROM upstreams ORDER BY name"
            )
        ]
        inflight = [
            dict(r) for r in _db().execute("SELECT * FROM inflight ORDER BY started_at")
        ]
        probes = [
            dict(r)
            for r in _db().execute(
                "SELECT * FROM probes ORDER BY created_at DESC LIMIT 10"
            )
        ]
        waiting = sorted(_waiters.values(), key=lambda w: w["since"])
    counts: dict[str, int] = {}
    for row in inflight:
        counts[row["target"]] = counts.get(row["target"], 0) + 1
    wait_counts: dict[str, int] = {}
    for w in waiting:
        wait_counts[w["target"]] = wait_counts.get(w["target"], 0) + 1
    now = time.time()
    for u in upstreams:
        u["inflight"] = counts.get(u["name"], 0)
        u["waiting"] = wait_counts.get(u["name"], 0)
        u["is_current"] = u["name"] == route["current_target"]
        # At/over quota: no further admission until occupancy drops below.
        u["full"] = u["inflight"] >= u["capacity"]
        # A non-current target may only be taken offline once its in-flight
        # requests have fully drained — never earlier, e.g. not merely
        # because its quota was lowered or the route moved away.
        u["can_offline"] = (not u["is_current"]) and u["inflight"] == 0
    for row in inflight:
        row["age_ms"] = int((now - row["started_at"]) * 1000)
    waiting = [dict(w) for w in waiting]
    for w in waiting:
        w["age_ms"] = int((now - w["since"]) * 1000)
    for p in probes:
        p["detail"] = json.loads(p["detail"]) if p["detail"] else {}
    return {
        "now": now,
        "route": route,
        "upstreams": upstreams,
        "inflight": inflight,
        "waiting": waiting,
        "recent_probes": probes,
    }
