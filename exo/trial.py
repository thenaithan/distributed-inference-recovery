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
import re
import subprocess
import sys
import time
from pathlib import Path

import urllib.error
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent
MODEL = "mlx-community/Qwen3-0.6B-8bit"
# exo keeps the SSE stream open with ": keep-alive" every 10 s while nothing happens, so a
# socket timeout never fires. Give up when no token has arrived for this long instead: well
# above exo's own stall detection (RUNNER_STALL_TIMEOUT 90 s), so it never pre-empts what we
# measure. Such trials end with end_kind "harness_timeout" = exo did not end the stream.
STALL_TIMEOUT = 300.0
# Healthy ranks connect within ~2 s of runner creation; a rank still Idle/Connecting this
# long after placement never will (no timeout in exo for that phase).
CONNECT_TIMEOUT = 90.0
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


def model_instances(st: dict) -> dict:
    """{instance_id: unwrapped instance} for every instance of MODEL in a /state payload."""
    out = {}
    for instance_id, wrapped in st.get("instances", {}).items():
        inst = tag(wrapped)[1]
        if inst.get("shardAssignments", {}).get("modelId") == MODEL:
            out[instance_id] = inst
    return out


def find_instance(st: dict, exclude: set = frozenset()) -> tuple[str, dict] | None:
    """The instance of MODEL this trial placed: the one not present before placement."""
    for instance_id, inst in model_instances(st).items():
        if instance_id not in exclude:
            return instance_id, inst
    return None


def clear_instances(base: str, timeout: float = 120.0) -> int:
    """Delete every instance of MODEL and wait until exo has dropped them. Each trial must
    start with none: a previous trial's recovery instance (or a smoke test's) would otherwise
    be found first, and exo might route the request to it, so the failure would miss."""
    st = api(base, "/state")
    leftover = set(model_instances(st))
    for instance_id in leftover:
        api(base, f"/instance/{instance_id}", method="DELETE")
        log(f"deleting leftover instance {instance_id}")
    deadline = now() + timeout
    while leftover and now() < deadline:
        time.sleep(0.5)
        st = poll_state(base)
        if st is not None:
            leftover &= set(model_instances(st))
    if leftover:
        raise RuntimeError(f"instances still present after {timeout:.0f}s: {sorted(leftover)}")
    return len(st.get("instances", {})) if st else 0


def place_and_wait(base: str, min_nodes: int, timeout: float = 600.0) -> dict:
    t0 = now()
    before = set(model_instances(api(base, "/state")))  # so a leftover is never mistaken for ours
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
        found = find_instance(st, exclude=before)
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
            # exo has no timeout on ConnectToGroup: if one rank never joins, the others block
            # in MLX's ring setup forever (seen 2026-10-21, the master node's runner stayed
            # Idle). Fail fast with the statuses rather than wait out the whole timeout.
            if (statuses and now() - t_placed > CONNECT_TIMEOUT
                    and all(s in (None, "RunnerIdle", "RunnerConnecting") for s in statuses.values())):
                raise RuntimeError(f"runners never connected within {CONNECT_TIMEOUT:.0f}s of placement "
                                   f"(statuses: {statuses}); exo is wedged, recreate the cluster: "
                                   f"./cluster.sh down && ./cluster.sh up")
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


ANSI = re.compile(r"\x1b\[[0-9;]*m")


def master_node(n: int, settle: float = 60.0) -> int | None:
    """Which node is master right now, from the nodes' own logs: the master logs
    "Node elected Master - maintaining/promoting self", followers "Node <id> elected master".
    exo exposes no API for it. The role moves with every election (a cluster restart, a
    master loss), so it is read per trial. A node that has just started declares itself
    master until it finds the others (about 3 s), so wait until exactly one node claims the
    role; None if the cluster has not settled within `settle` seconds."""
    deadline = now() + settle
    while True:
        masters = []
        for i in range(n):
            out = subprocess.run(["podman", "logs", "--tail", "5000", f"exo-{i}"], capture_output=True, text=True)
            lines = [ANSI.sub("", l) for l in (out.stdout + out.stderr).splitlines() if "elected" in l]
            if lines and "Node elected Master" in lines[-1]:
                masters.append(i)
        if len(masters) == 1:
            return masters[0]
        if now() >= deadline:
            log(f"no single master after {settle:.0f}s (self-declared: {masters})")
            return None
        log(f"waiting for the cluster to agree on a master (self-declared: {masters})")
        time.sleep(2.0)


def node_ids(n: int) -> list:
    """Each node's NodeId from its own API (cluster.sh publishes node i on 52415+i), so
    records can be matched to log lines, which name nodes by id. None where unreachable."""
    ids = []
    for i in range(n):
        try:
            ids.append(api(f"http://localhost:{52415 + i}", "/node_id", timeout=3))
        except Exception:
            ids.append(None)
    return ids


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
    ap.add_argument("--allow-api-node", action="store_true",
                    help="fail the node whose API serves the stream (measures a connection drop, not detection)")
    args = ap.parse_args()
    base = args.base
    # The stream must come from a survivor: if the failed node is also the API node, the
    # stream ends by connection drop and 'detect' measures nothing about exo's detectors.
    # (For master loss: --base on a survivor, kill the master; detection is then the 30 s
    # heartbeat silence plus the election, as in trial 1.)
    api_node = (urllib.parse.urlsplit(base).port or 80) - 52415
    if args.mode != "none" and api_node == args.fail_node and not args.allow_api_node:
        raise SystemExit(f"--base is node {api_node}'s API and --fail-node is {args.fail_node}: the stream would die "
                         f"by connection drop. Use --base http://localhost:{52415 + (args.fail_node + 1) % args.nodes} "
                         f"(a survivor), or --allow-api-node to measure the drop on purpose.")

    rec: dict = dict(label=args.label, model=MODEL, nodes=args.nodes, fail_node=args.fail_node,
                     mode=args.mode, at_token=args.at_token, stall_timeout=args.stall_timeout,
                     host_time=now(), cpusets=node_cpusets(args.nodes))

    if args.mode != "none":  # which role the failed node had: worker loss and master loss differ
        rec.update(master_node=master_node(args.nodes), master_source="podman logs", node_ids=node_ids(args.nodes))
        role = "master" if rec["master_node"] == args.fail_node else "worker" if rec["master_node"] is not None else "unknown role"
        log(f"master is node {rec['master_node']}; failing node {args.fail_node} ({role})")

    clear_instances(base)
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
