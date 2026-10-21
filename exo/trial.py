#!/usr/bin/env python3
"""One failure trial against a running exo cluster: measure C and its parts.

    python3 trial.py --nodes 3 --fail-node 1 --mode kill --out trials.jsonl
    (defaults: inject after 5 output tokens, request 30; C does not depend on where the
     failure lands, and on CPU every token costs seconds)

What it does, with wall-clock timestamps on every step (time.time(), one host, one clock):

  1. place an instance of MODEL with Pipeline sharding over `--nodes` nodes, wait ready
  2. start a streaming chat completion (SSE); timestamp every token
  3. at token `--at-token`, inject the failure on node `--fail-node` via cluster.sh
     (kill = SIGKILL, pause = SIGSTOP, cut = network disconnect)
  4. keep reading the stream until it ends (error chunk or normal end); timestamp that
  5. poll GET /state until the instance is gone (InstanceDeleted), timestamp that
  6. re-place the instance over the surviving nodes (nodes-1), wait ready, timestamp
  7. resubmit the same prompt; timestamp the first token
  8. undo the failure (resume / heal; a killed container is restarted by cluster.sh up)

Records one JSON line per trial. Nothing here is specific to our simulation; analyze.py
turns these into detect / recover / replay components and churnsim parameters.

The master has been node 0 in every run so far (its log says "Node elected Master"; check
it before master-loss trials). `--fail-node 0` measures master loss instead of worker loss;
expect an election rather than an error chunk. Talk to a survivor for those trials:
cluster.sh publishes node i's API on localhost:52415+i, so `--base http://localhost:52416`.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import urllib.error
import urllib.request

HERE = Path(__file__).resolve().parent
MODEL = "mlx-community/Qwen3-0.6B-8bit"
# exo keeps the SSE stream open with ": keep-alive" every 10 s while nothing happens, so a
# socket timeout never fires. Give up when no token has arrived for this long instead: well
# above exo's own stall detection (RUNNER_STALL_TIMEOUT 90 s), so it never pre-empts what we
# measure. Such trials end with end_kind "harness_timeout" = exo did not end the stream.
STALL_TIMEOUT = 300.0
PROMPT = "Write a long, detailed, numbered list of 60 unrelated facts about rivers, one per line."


def now() -> float:
    return time.time()


PROGRESS_EVERY = 10  # tokens between progress lines
USAGE: dict = {}     # last "usage" object seen on a stream (prompt_tokens, completion_tokens)
WARMUP_TOKENS = 50   # exo's warmup decodes this many tokens before RunnerReady (runner logs)


def log(msg: str) -> None:
    """Progress to stderr, so a slow run is visibly alive; stdout keeps the summary."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def api(base: str, path: str, body=None, method=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method or ("POST" if data else "GET"),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode() or "null")
    except urllib.error.HTTPError as e:
        # exo puts the reason in the body ({"detail": ...}); urllib's message drops it
        raise ApiError(e.code, path, e.read().decode(errors="replace")) from None


class ApiError(Exception):
    def __init__(self, code: int, path: str, body: str):
        super().__init__(f"HTTP {code} from {path}: {body}")
        self.code = code


def place(base: str, min_nodes: int, deadline: float) -> float:
    """POST /place_instance, retrying while exo refuses with 400 (it checks the request
    against its current topology, so it refuses until the nodes have found each other,
    and again after a failure until the dead node is dropped). Returns when it was first
    tried; raises with exo's last reason if it is still refused at the deadline."""
    t_first = now()
    while True:
        try:
            api(base, "/place_instance", {"model_id": MODEL, "sharding": "Pipeline",
                                          "instance_meta": "MlxRing", "min_nodes": min_nodes})
            return t_first
        except ApiError as e:
            if e.code != 400 or now() >= deadline:
                raise
            time.sleep(1.0)


# exo serializes state with camelCase aliases and wraps each tagged-union value in its class
# name: instances are {id: {"MlxRingInstance": {...}}}, runners {id: {"RunnerReady": {...}}}.
# Ready is RunnerReady or RunnerRunning, the test exo's own worker uses (worker/plan.py).
READY = {"RunnerReady", "RunnerRunning"}


def tag(wrapped) -> tuple[str | None, dict]:
    if isinstance(wrapped, dict) and len(wrapped) == 1:
        (name, inner), = wrapped.items()
        return name, inner
    return None, {}


def find_instance(st: dict) -> tuple[str, dict] | None:
    for instance_id, wrapped in st.get("instances", {}).items():
        inst = tag(wrapped)[1]
        if inst.get("shardAssignments", {}).get("modelId") == MODEL:
            return instance_id, inst
    return None


def place_and_wait(base: str, min_nodes: int, timeout: float = 600.0) -> dict:
    t0 = now()
    deadline = t0 + timeout
    t_try = place(base, min_nodes, deadline)
    t_cmd = now()
    # /instance/await streams one SSE line when an instance for the model exists in state;
    # "exists" is placement, not warm. We then poll /state for a ready runner.
    req = urllib.request.Request(f"{base}/instance/await?model_id={MODEL}&timeout_seconds=300")
    with urllib.request.urlopen(req, timeout=320) as r:
        for line in r:
            if line.startswith(b"data:"):
                break
    t_placed = now()
    statuses: dict = {}
    timeline: list = []  # [t, {runner_id: status}] on every change: load vs warmup split
    while now() < deadline:
        st = poll_state(base)
        if st is None:
            time.sleep(0.5)
            continue
        found = find_instance(st)
        if found:
            instance_id, inst = found
            runners = st.get("runners", {})
            statuses = {rid: tag(runners.get(rid))[0]
                        for rid in inst.get("shardAssignments", {}).get("runnerToShard", {})}
            if not timeline or timeline[-1][1] != statuses:
                timeline.append([now(), dict(statuses)])
            for rid, status in statuses.items():
                if status == "RunnerFailed":
                    raise RuntimeError(f"runner {rid} failed: {tag(runners[rid])[1]}")
            if statuses and all(s in READY for s in statuses.values()):
                return dict(t_place_try=t_try, t_place_cmd=t_cmd, t_placed=t_placed, t_ready=now(),
                            instance_id=instance_id, status_timeline=timeline)
        time.sleep(0.25)
    raise RuntimeError(f"instance not ready within timeout (runners: {statuses or 'no instance'}); "
                       "check ./cluster.sh logs 0")


def stream(base: str, on_token, max_tokens: int = 30, stall_timeout: float = STALL_TIMEOUT):
    """Run a streaming chat completion; call on_token(index, t) per content chunk.
    Returns (n_tokens, t_end, end_kind, last_payload). Ends with "harness_timeout" when no
    token arrives for stall_timeout seconds (counted from the request until the first)."""
    # enable_thinking=False: Qwen3 thinks by default, and exo streams thinking tokens as
    # delta.reasoning_content, so a short request could produce no content at all and the
    # failure would never be injected. temperature 0 matches vllm/trial.py.
    body = {"model": MODEL, "stream": True, "max_tokens": max_tokens, "temperature": 0,
            "enable_thinking": False,
            "stream_options": {"include_usage": True},
            "messages": [{"role": "user", "content": PROMPT}]}
    req = urllib.request.Request(base + "/v1/chat/completions", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    n, last = 0, None
    t_progress = now()
    try:
        with urllib.request.urlopen(req, timeout=stall_timeout) as r:
            for raw in r:
                if now() - t_progress > stall_timeout:
                    return n, now(), "harness_timeout", last
                if not raw.startswith(b"data:"):
                    continue
                payload = raw[5:].strip()
                if payload == b"[DONE]":
                    return n, now(), "done", last
                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict) and obj.get("usage"):
                    USAGE.update(obj["usage"])  # prompt_tokens: the prefill size, in tokens
                last = obj
                if "error" in obj or obj.get("object") == "error":
                    return n, now(), "error_chunk", obj
                choices = obj.get("choices") or []
                if choices:
                    delta = choices[0].get("delta", {})
                    # every generated token counts, thinking or not (usage splits them)
                    if delta.get("content") or delta.get("reasoning_content"):
                        n += 1
                        t_progress = now()
                        on_token(n, t_progress)
                    if choices[0].get("finish_reason"):
                        return n, now(), "finish_reason", obj
    except TimeoutError:  # not a byte, not even a keep-alive, for stall_timeout
        return n, now(), "harness_timeout", last
    except Exception as e:  # connection dropped, HTTP error
        return n, now(), f"exception:{type(e).__name__}", {"message": str(e)}
    return n, now(), "eof", last


def node_cpusets(n: int) -> list:
    """CPU set each node container actually runs on (cluster.sh EXO_CPUSETS), for the record."""
    sets = []
    for i in range(n):
        out = subprocess.run(["podman", "inspect", "-f", "{{.HostConfig.CpusetCpus}}", f"exo-{i}"],
                             capture_output=True, text=True)
        sets.append(out.stdout.strip() if out.returncode == 0 else None)
    return sets


def inject(mode: str, node: int) -> float:
    out = subprocess.run([str(HERE / "cluster.sh"), mode, str(node)], capture_output=True, text=True, check=True)
    return float(out.stdout.strip().splitlines()[0])


def poll_state(base: str) -> dict | None:
    """GET /state, or None on a transient failure: exo pauses its API during an election,
    and the polled node may itself be the one that just failed. Callers keep polling."""
    try:
        state = api(base, "/state", timeout=5)
        _UNAVAILABLE_SINCE.pop(base, None)
        return state
    except (ApiError, OSError) as e:  # URLError, connection reset and timeouts are OSErrors
        since = _UNAVAILABLE_SINCE.setdefault(base, now())
        if now() - _LAST_UNAVAILABLE_LOG.get(base, 0) >= 10:  # log once per 10 s of outage
            _LAST_UNAVAILABLE_LOG[base] = now()
            log(f"/state unavailable for {now() - since:.0f}s ({type(e).__name__}); retrying")
        return None


_UNAVAILABLE_SINCE: dict = {}
_LAST_UNAVAILABLE_LOG: dict = {}


def wait_instance_gone(base: str, instance_id: str, timeout: float = 120.0) -> float | None:
    deadline = now() + timeout
    while now() < deadline:
        st = poll_state(base)
        if st is not None and instance_id not in st.get("instances", {}):
            return now()
        time.sleep(0.25)
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:52415")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--fail-node", type=int, default=1)
    ap.add_argument("--mode", choices=["kill", "pause", "cut", "none"], default="kill")
    ap.add_argument("--at-token", type=int, default=5,
                    help="inject after this many output tokens (early: C does not depend on it)")
    ap.add_argument("--max-tokens", type=int, default=30,
                    help="output tokens requested; on CPU each costs ~2.5 s")
    ap.add_argument("--out", default="trials.jsonl")
    ap.add_argument("--label", default="")
    ap.add_argument("--stall-timeout", type=float, default=STALL_TIMEOUT,
                    help="end the stream after this many seconds without a token")
    args = ap.parse_args()
    base = args.base

    rec: dict = dict(label=args.label, model=MODEL, nodes=args.nodes, fail_node=args.fail_node,
                     mode=args.mode, at_token=args.at_token, stall_timeout=args.stall_timeout,
                     host_time=now(), cpusets=node_cpusets(args.nodes))

    placed = place_and_wait(base, args.nodes)
    rec.update({k: v for k, v in placed.items() if k != "status_timeline"},
               place_status_timeline=placed["status_timeline"])
    instance_id = placed["instance_id"]

    tokens: list = []
    state = {"injected": None}

    def on_token(i, t):
        tokens.append(t)
        if args.mode != "none" and i == args.at_token and state["injected"] is None:
            state["injected"] = inject(args.mode, args.fail_node)
            log(f"injected {args.mode} on node {args.fail_node} at token {i}")
        if i == 1:
            log(f"first token {t - t_req:.1f}s after the request")
        elif i % PROGRESS_EVERY == 0:
            log(f"{i} tokens, {(t - tokens[0]) / (i - 1):.2f}s/token")

    log(f"instance ready ({placed['t_ready'] - placed['t_place_try']:.0f}s); streaming")
    t_req = now()
    USAGE.clear()
    n, t_end, kind, last = stream(base, on_token, max_tokens=args.max_tokens, stall_timeout=args.stall_timeout)
    rec.update(t_request=t_req, tokens_before_end=n, t_first_token=tokens[0] if tokens else None,
               max_tokens=args.max_tokens, usage=dict(USAGE), prompt_chars=len(PROMPT),
               warmup_tokens=WARMUP_TOKENS,
               t_fail=state["injected"], t_stream_end=t_end, end_kind=kind,
               t_last_token=tokens[-1] if tokens else None,
               inter_token_s=[round(b - a, 4) for a, b in zip(tokens, tokens[1:])],
               last_payload=last)

    if args.mode != "none":
        rec["t_instance_gone"] = wait_instance_gone(base, instance_id)
        # recover: re-place on the survivors and resubmit
        survivors = args.nodes - 1
        try:
            re = place_and_wait(base, survivors)
            rec.update(t_replace_try=re["t_place_try"], t_replace_cmd=re["t_place_cmd"],
                       t_replaced=re["t_placed"], t_replace_ready=re["t_ready"],
                       replace_status_timeline=re["status_timeline"])
            first = {}
            USAGE.clear()
            t_resub = now()
            n2, t_end2, kind2, _ = stream(base, lambda i, t: first.setdefault("t", t), max_tokens=5,
                                      stall_timeout=args.stall_timeout)
            rec.update(t_resubmit_request=t_resub, t_resubmit_first_token=first.get("t"),
                       resubmit_end_kind=kind2, resubmit_usage=dict(USAGE))
        except Exception as e:
            rec["recover_error"] = f"{type(e).__name__}: {e}"
        # undo
        if args.mode == "pause":
            inject("resume", args.fail_node)
        elif args.mode == "cut":
            inject("heal", args.fail_node)
        # a killed container stays down; restart it with ./cluster.sh up before the next trial

    with open(args.out, "a") as f:
        f.write(json.dumps(rec) + "\n")
    summary = {k: rec[k] for k in ("mode", "tokens_before_end", "end_kind", "t_fail",
                                   "t_stream_end", "t_instance_gone", "t_replace_ready",
                                   "t_resubmit_first_token") if k in rec}
    err = (last or {}).get("error")
    if err:  # expected after a failure; with --mode none it means the plumbing is broken
        msg = err.get("message", err) if isinstance(err, dict) else err
        summary["error"] = str(msg)[:500]
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    sys.exit(main())
