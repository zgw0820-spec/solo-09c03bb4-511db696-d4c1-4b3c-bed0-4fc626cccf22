#!/usr/bin/env python3
"""End-to-end smoke test for the edge gateway.

Usage:
  smoke_test.py BASE_URL SAVE_FILE phase1   # switch + quota semantics, arms an interrupted request
  smoke_test.py BASE_URL SAVE_FILE phase2   # post-restart recovery checks
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SAVE = sys.argv[2] if len(sys.argv) > 2 else "/tmp/edge-gw-phase1.json"
PHASE = sys.argv[3] if len(sys.argv) > 3 else "phase1"

FAILURES: list[str] = []


def api(method, path, body=None, timeout=70):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, method=method, data=data,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(name, cond, extra=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def inflight_of(state, name):
    for u in state["upstreams"]:
        if u["name"] == name:
            return u["inflight"]
    return 0


def waiting_of(state, name):
    for u in state["upstreams"]:
        if u["name"] == name:
            return u.get("waiting", 0)
    return 0


def total_inflight(state):
    return sum(u["inflight"] for u in state["upstreams"])


def total_waiting(state):
    return sum(u.get("waiting", 0) for u in state["upstreams"])


def upstream_of(state, name):
    return next(u for u in state["upstreams"] if u["name"] == name)


def wait_state(pred, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, st = api("GET", "/api/state")
        if pred(st):
            return st
        time.sleep(0.1)
    check(f"等待状态: {what}", False)
    return api("GET", "/api/state")[1]


def call_later(results, delay_ms, note=""):
    """Fire one proxied request in the background; append its result dict."""
    def _run():
        try:
            s, b = api("POST", "/api/call", {"delay_ms": delay_ms})
            results.append({"status": s, "body": b, "note": note})
        except Exception as e:  # noqa: BLE001
            results.append({"error": str(e), "note": note})
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def wait_result(results, what, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if results:
            return results[0]
        time.sleep(0.05)
    check(f"等待结果: {what}", False)
    return {}


def set_cap(name, cap):
    s, b = api("POST", f"/api/upstreams/{name}/capacity", {"capacity": cap})
    check(f"设置 {name} 名额={cap}", s == 200 and b.get("ok") is True
          and b.get("capacity") == cap, f"status={s} body={b}")
    return b


def switch_to(target):
    """Probe + commit at the current route version; returns the commit body."""
    _, st = api("GET", "/api/state")
    v = st["route"]["version"]
    s, p = api("POST", "/api/switch/probe", {"target": target, "base_version": v})
    if not p.get("ok"):
        return {"ok": False, "error": f"probe_failed: {p}"}
    s, c = api("POST", "/api/switch/commit",
               {"target": target, "base_version": v, "probe_id": p["probe_id"]})
    return c


def phase1():
    _, st = api("GET", "/api/state")
    check("初始当前目标为 alpha", st["route"]["current_target"] == "alpha")
    check("初始版号为 0", st["route"]["version"] == 0)
    check("初始无在途请求", len(st["inflight"]) == 0)
    check("初始无等候请求", total_waiting(st) == 0)
    check("初始名额为默认值 3", all(u["capacity"] == 3 for u in st["upstreams"]))

    # 1) slow request admitted to alpha, keeps running through the switch
    slow = {}

    def slow_call():
        try:
            s, b = api("POST", "/api/call", {"delay_ms": 4000})
            slow["status"], slow["body"] = s, b
        except Exception as e:  # noqa: BLE001
            slow["error"] = str(e)

    t = threading.Thread(target=slow_call, daemon=True)
    t.start()
    wait_state(lambda s: inflight_of(s, "alpha") == 1, "alpha 在途变为 1")

    # 2) before any switch, a fast call still goes to alpha
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换前快请求走 alpha", r["target"] == "alpha" and r["upstream"]["upstream"] == "alpha")

    # 3) probe beta; while/after probing (before commit) traffic stays on alpha
    s, p = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 0})
    check("探测 beta 成功", p["ok"] is True and p["version_matches"] is True)
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("探测后未确认前请求仍走 alpha", r["target"] == "alpha")
    _, st = api("GET", "/api/state")
    check("探测不改变路由", st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)

    # 4) commit the switch
    s, c = api("POST", "/api/switch/commit",
               {"target": "beta", "base_version": 0, "probe_id": p["probe_id"]})
    check("确认切换成功，版号升为 1", c.get("ok") is True and c.get("version") == 1)

    # 5) new requests immediately go to beta, no waiting for the old one
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换后新请求立即走 beta", r["target"] == "beta" and r["upstream"]["upstream"] == "beta")
    _, st = api("GET", "/api/state")
    check("旧目标 alpha 仍有 1 条在途（未等待其结束）", inflight_of(st, "alpha") == 1)
    check("alpha 在途未清零，不可下线", upstream_of(st, "alpha")["can_offline"] is False)

    # 6) stale commit (page version outdated) must not override the effective route
    s, p_stale = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 0})
    check("过期版号的探测被标记 version_matches=false",
          p_stale["ok"] is True and p_stale["version_matches"] is False)
    s, c_stale = api("POST", "/api/switch/commit",
                     {"target": "alpha", "base_version": 0, "probe_id": p_stale["probe_id"]})
    check("过期版号的确认被拒绝(409 version_conflict)",
          s == 409 and c_stale["error"] == "version_conflict")
    _, st = api("GET", "/api/state")
    check("路由未被迟到提交覆盖，仍为 beta@v1",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == 1)

    # 7) failed probe keeps the original route
    s, pd = api("POST", "/api/switch/probe", {"target": "dead", "base_version": 1})
    check("探测不可达候选失败", pd["ok"] is False)
    _, st = api("GET", "/api/state")
    check("探测失败后维持原路由 beta@v1",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == 1)
    s, cd = api("POST", "/api/switch/commit",
                {"target": "dead", "base_version": 1, "probe_id": pd["probe_id"]})
    check("失败探测对应的确认被拒绝", s == 400 and cd["error"] == "probe_invalid")

    # 8) two interleaved switches on the same base version: exactly one wins
    s, px = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 1})
    s, py = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 1})
    s, cx = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": 1, "probe_id": px["probe_id"]})
    check("交错切换：先到的确认生效(v2)", cx.get("ok") is True and cx.get("version") == 2)
    s, cy = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": 1, "probe_id": py["probe_id"]})
    check("交错切换：迟到的确认被版号拒绝", s == 409 and cy["error"] == "version_conflict")

    # 9) late probe must not overwrite an already-effective route
    s, pz = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 2})
    check("探测 beta@v2 成功", pz["ok"] is True)
    s, pz2 = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 2})
    s, cz2 = api("POST", "/api/switch/commit",
                 {"target": "beta", "base_version": 2, "probe_id": pz2["probe_id"]})
    check("另一值班员先完成切换 beta@v3", cz2.get("ok") is True and cz2.get("version") == 3)
    s, cz = api("POST", "/api/switch/commit",
                {"target": "beta", "base_version": 2, "probe_id": pz["probe_id"]})
    check("迟到探测的确认不得覆盖已生效路由", s == 409 and cz["error"] == "version_conflict")
    _, st = api("GET", "/api/state")
    check("路由保持 beta@v3", st["route"]["current_target"] == "beta" and st["route"]["version"] == 3)

    # 10) the slow pre-switch request completes with the full response from alpha
    t.join(timeout=15)
    check("慢请求已结束", "body" in slow, slow.get("error", ""))
    body = slow.get("body", {})
    check("慢请求从原上游 alpha 取回完整响应",
          body.get("ok") is True and body.get("upstream", {}).get("upstream") == "alpha"
          and body.get("upstream", {}).get("delay_ms") == 4000)

    # 11) alpha drains to zero -> only then may it be taken offline
    wait_state(lambda s: inflight_of(s, "alpha") == 0, "alpha 在途清零")
    _, st = api("GET", "/api/state")
    check("alpha 在途清零后提示可下线", upstream_of(st, "alpha")["can_offline"] is True)
    check("当前目标 beta 不标记可下线", upstream_of(st, "beta")["can_offline"] is False)

    # ==================== 并发名额（quota） ====================
    # Route is now beta@v3. Quota bounds are validated first.
    s, _ = api("POST", "/api/upstreams/alpha/capacity", {"capacity": 0})
    check("名额下限校验(0 被拒)", s == 422)
    s, _ = api("POST", "/api/upstreams/alpha/capacity", {"capacity": 6})
    check("名额上限校验(6 被拒)", s == 422)
    s, _ = api("POST", "/api/upstreams/ghost/capacity", {"capacity": 2})
    check("未知目标名额设置被拒(404)", s == 404)

    # A) full quota -> excess request waits, times out busy, never forwarded
    set_cap("beta", 1)
    res_slow, res_wait = [], []
    call_later(res_slow, 6000, "A-slow")          # holds beta's only slot
    wait_state(lambda s: inflight_of(s, "beta") == 1, "A: beta 在途为 1")
    call_later(res_wait, 50, "A-waiter")          # must queue
    wait_state(lambda s: waiting_of(s, "beta") == 1, "A: beta 等候数为 1")
    _, st = api("GET", "/api/state")
    check("A: 名额占满时在途不超配", inflight_of(st, "beta") <= 1)
    check("A: 名额占满时状态标记已满", upstream_of(st, "beta")["full"] is True)
    r_wait = wait_result(res_wait, "A: 等候者超时", timeout=8)
    check("A: 等候 5s 超时返回忙碌(503 busy)",
          r_wait.get("status") == 503 and r_wait.get("body", {}).get("error") == "busy",
          f"got {r_wait.get('status')} {r_wait.get('body')}")
    waited = r_wait.get("body", {}).get("waited_ms", 0)
    check("A: 等候时长约 5s", 4500 <= waited <= 6500, f"waited_ms={waited}")
    check("A: 超时请求未被转发", "upstream" not in r_wait.get("body", {}))
    _, st = api("GET", "/api/state")
    check("A: 超时后等候记录不留占位", waiting_of(st, "beta") == 0)
    check("A: 超时后名额占用不变(仅剩慢请求)", inflight_of(st, "beta") == 1)
    r_slow = wait_result(res_slow, "A: 慢请求完成", timeout=8)
    check("A: 占名额的慢请求不受影响完整返回",
          r_slow.get("body", {}).get("ok") is True
          and r_slow["body"]["upstream"]["upstream"] == "beta")

    # B) a waiting request immediately competes for the NEW target's quota
    #    once a switch commits; admitted requests still finish on the old one
    set_cap("alpha", 2)
    res_slowB, res_sw = [], []
    call_later(res_slowB, 4000, "B-slow")         # holds beta's only slot
    wait_state(lambda s: inflight_of(s, "beta") == 1, "B: beta 在途为 1")
    t0 = time.time()
    call_later(res_sw, 100, "B-waiter")           # queues for beta
    wait_state(lambda s: waiting_of(s, "beta") == 1, "B: beta 等候数为 1")
    c = switch_to("alpha")                        # beta@v3 -> alpha@v4
    check("B: 等待期间确认切换成功", c.get("ok") is True, str(c))
    r_sw = wait_result(res_sw, "B: 等候者完成", timeout=5)
    dt = time.time() - t0
    check("B: 等候中请求转向新目标 alpha 完成",
          r_sw.get("body", {}).get("ok") is True
          and r_sw["body"].get("target") == "alpha",
          f"got {r_sw.get('status')} {r_sw.get('body')}")
    check("B: 转向迅速(未等满 5s 超时)", dt < 3, f"dt={dt:.2f}s")
    r_slowB = wait_result(res_slowB, "B: 慢请求完成", timeout=8)
    check("B: 已接纳请求仍从原目标 beta 取回完整响应",
          r_slowB.get("body", {}).get("ok") is True
          and r_slowB["body"].get("target") == "beta"
          and r_slowB["body"]["upstream"]["upstream"] == "beta")

    # C) lowering the quota never interrupts in-flight requests and blocks
    #    new admissions until occupancy drops below the new quota
    #    (route is alpha@v4 now, alpha quota is 2)
    res_c1, res_c2, res_c3 = [], [], []
    call_later(res_c1, 3000, "C-1")
    call_later(res_c2, 5500, "C-2")
    wait_state(lambda s: inflight_of(s, "alpha") == 2, "C: alpha 在途为 2")
    b = set_cap("alpha", 1)
    check("C: 调低名额后在途请求不被打断(仍 2 条)", b.get("inflight") == 2)
    _, st = api("GET", "/api/state")
    check("C: 在途超过新名额时标记已满(不再接纳)", upstream_of(st, "alpha")["full"] is True)
    call_later(res_c3, 50, "C-3")                 # queues; c1 frees at t=3s but
    wait_state(lambda s: waiting_of(s, "alpha") == 1, "C: alpha 等候数为 1")
    r_c3 = wait_result(res_c3, "C: 超额请求超时", timeout=8)   # occupancy 1 >= cap 1
    check("C: 调低名额后超额请求等候超时返回忙碌",
          r_c3.get("status") == 503 and r_c3.get("body", {}).get("error") == "busy",
          f"got {r_c3.get('status')} {r_c3.get('body')}")
    r_c1 = wait_result(res_c1, "C: 在途请求 1 完成", timeout=8)
    r_c2 = wait_result(res_c2, "C: 在途请求 2 完成", timeout=8)
    check("C: 两条在途请求均完整完成未被打断",
          r_c1.get("body", {}).get("ok") is True and r_c2.get("body", {}).get("ok") is True)

    # D) quota edits + admissions + a switch interleaved: never over-committed,
    #    never misrouted (response target must match its admission route version)
    set_cap("alpha", 2)
    set_cap("beta", 2)
    wait_state(lambda s: total_inflight(s) == 0 and total_waiting(s) == 0,
               "D: 开始前在途与等候清零")
    _, st = api("GET", "/api/state")
    v_before = st["route"]["version"]             # alpha@v_before
    samples, stop = [], threading.Event()

    def sampler():
        while not stop.is_set():
            _, s_ = api("GET", "/api/state")
            for u in s_["upstreams"]:
                samples.append((u["name"], u["inflight"], u["capacity"]))
            time.sleep(0.06)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    res_d, threads = [], []
    for i in range(8):
        threads.append(call_later(res_d, 800, f"D-{i}"))
    time.sleep(0.15)                              # alpha full, 6 queued
    c = switch_to("beta")                         # alpha@v_before -> beta
    check("D: 压力期间切换成功", c.get("ok") is True, str(c))
    v_after = c.get("version", v_before + 1)
    time.sleep(0.2)
    set_cap("beta", 3)                            # interleave a quota raise
    for t_ in threads:
        t_.join(timeout=15)
    stop.set()
    th.join(timeout=3)
    over = [s_ for s_ in samples if s_[1] > s_[2]]
    check("D: 名额/接纳/切换交错全程未超配", not over, str(over[:3]))
    check("D: 8 个并发请求全部完成", len(res_d) == 8
          and all(r.get("body", {}).get("ok") is True for r in res_d),
          f"results={[(r.get('status'), r.get('note')) for r in res_d]}")
    valid = {("alpha", v_before), ("beta", v_after)}
    bad = [r for r in res_d
           if r.get("body", {}).get("ok")
           and (r["body"].get("target"), r["body"].get("route_version")) not in valid]
    check("D: 响应目标与接纳时路由版号一致(无错投)", not bad, str(bad[:2]))
    targets = {r["body"].get("target") for r in res_d if r.get("body", {}).get("ok")}
    check("D: 切换后等候者转向新目标 beta", "beta" in targets, str(targets))

    # E) restore defaults and drain before arming the interrupted request
    set_cap("alpha", 3)
    set_cap("beta", 3)
    wait_state(lambda s: total_inflight(s) == 0 and total_waiting(s) == 0,
               "E: 名额测试后在途与等候全部清零", timeout=15)

    # 12) arm one in-flight request that the harness will interrupt with kill -9
    _, st = api("GET", "/api/state")
    armed_target = st["route"]["current_target"]

    def interrupted():
        try:
            api("POST", "/api/call", {"delay_ms": 15000})
        except Exception:  # noqa: BLE001 - connection reset when gateway dies
            pass

    threading.Thread(target=interrupted, daemon=True).start()
    wait_state(lambda s: inflight_of(s, armed_target) == 1,
               f"重启前 {armed_target} 在途为 1")
    _, st = api("GET", "/api/state")
    with open(SAVE, "w") as f:
        json.dump({"target": st["route"]["current_target"],
                   "version": st["route"]["version"]}, f)
    print(f"PHASE1 READY: 已在 {armed_target} 上挂起 1 条在途请求，等待网关被杀")

    # keep the client connection open until the gateway is killed
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            api("GET", "/api/state", timeout=2)
            time.sleep(0.2)
        except Exception:  # noqa: BLE001
            print("PHASE1 OK: 网关已停止")
            return
    check("等待网关被杀", False)


def phase2():
    with open(SAVE) as f:
        saved = json.load(f)
    _, st = api("GET", "/api/state")
    check("重启后按已登记路由接流量(目标不变)", st["route"]["current_target"] == saved["target"])
    check("重启后路由版号保持", st["route"]["version"] == saved["version"])
    check("中断请求未留作在途", len(st["inflight"]) == 0)
    check("重启后无等候占位", total_waiting(st) == 0)
    check("启动时清理了中断在途记录", st["boot"]["cleared_inflight"] >= 1)
    check("重启后名额配置已持久化",
          upstream_of(st, "alpha")["capacity"] == 3
          and upstream_of(st, "beta")["capacity"] == 3)
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("重启后新请求正常发往已登记目标",
          r["target"] == saved["target"] and r["ok"] is True)


if PHASE == "phase1":
    phase1()
else:
    phase2()

if FAILURES:
    print(f"\n{len(FAILURES)} 项检查失败:")
    for f in FAILURES:
        print(" -", f)
    sys.exit(1)
print(f"\n全部检查通过 ({PHASE})")
