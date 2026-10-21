#!/usr/bin/env python3
"""One failure trial against a running exo cluster: measure C and its parts.

    python3 trial.py --nodes 3 --fail-node 1 --mode kill --at-token 40 --out trials.jsonl

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

The master is node 0 by default (it has the published API port). `--fail-node 0` measures
master loss instead of worker loss; expect an election rather than an error chunk.
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
PROMPT = "Write a long, detailed, numbered list of 60 unrelated facts about rivers, one per line."


def now() -> float:
    return time.time()


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
    while now() < deadline:
        st = api(base, "/state")
        found = find_instance(st)
        if found:
            instance_id, inst = found
            runners = st.get("runners", {})
            statuses = {rid: tag(runners.get(rid))[0]
                        for rid in inst.get("shardAssignments", {}).get("runnerToShard", {})}
            for rid, status in statuses.items():
                if status == "RunnerFailed":
                    raise RuntimeError(f"runner {rid} failed: {tag(runners[rid])[1]}")
            if statuses and all(s in READY for s in statuses.values()):
                return dict(t_place_try=t_try, t_place_cmd=t_cmd, t_placed=t_placed, t_ready=now(),
                            instance_id=instance_id)
        time.sleep(0.5)
    raise RuntimeError(f"instance not ready within timeout (runners: {statuses or 'no instance'}); "
                       "check ./cluster.sh logs 0")


def stream(base: str, on_token, max_tokens: int = 400):
    """Run a streaming chat completion; call on_token(index, t) per content chunk.
    Returns (n_tokens, t_end, end_kind, last_payload)."""
    body = {"model": MODEL, "stream": True, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": PROMPT}]}
    req = urllib.request.Request(base + "/v1/chat/completions", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    n, last = 0, None
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            for raw in r:
                if not raw.startswith(b"data:"):
                    continue
                payload = raw[5:].strip()
                if payload == b"[DONE]":
                    return n, now(), "done", last
                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                last = obj
                if "error" in obj or obj.get("object") == "error":
                    return n, now(), "error_chunk", obj
                choices = obj.get("choices") or []
                if choices and (choices[0].get("delta", {}).get("content") or choices[0].get("finish_reason")):
                    if choices[0].get("delta", {}).get("content"):
                        n += 1
                        on_token(n, now())
                    if choices[0].get("finish_reason"):
                        return n, now(), "finish_reason", obj
    except Exception as e:  # connection dropped, HTTP error, timeout
        return n, now(), f"exception:{type(e).__name__}", {"message": str(e)}
    return n, now(), "eof", last


def inject(mode: str, node: int) -> float:
    out = subprocess.run([str(HERE / "cluster.sh"), mode, str(node)], capture_output=True, text=True, check=True)
    return float(out.stdout.strip().splitlines()[0])


def wait_instance_gone(base: str, instance_id: str, timeout: float = 120.0) -> float | None:
    deadline = now() + timeout
    while now() < deadline:
        st = api(base, "/state")
        if instance_id not in st.get("instances", {}):
            return now()
        time.sleep(0.25)
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:52415")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--fail-node", type=int, default=1)
    ap.add_argument("--mode", choices=["kill", "pause", "cut", "none"], default="kill")
    ap.add_argument("--at-token", type=int, default=40)
    ap.add_argument("--out", default="trials.jsonl")
    ap.add_argument("--label", default="")
    args = ap.parse_args()
    base = args.base

    rec: dict = dict(label=args.label, model=MODEL, nodes=args.nodes, fail_node=args.fail_node,
                     mode=args.mode, at_token=args.at_token, host_time=now())

    placed = place_and_wait(base, args.nodes)
    rec.update(placed)
    instance_id = placed["instance_id"]

    tokens: list = []
    state = {"injected": None}

    def on_token(i, t):
        tokens.append(t)
        if args.mode != "none" and i == args.at_token and state["injected"] is None:
            state["injected"] = inject(args.mode, args.fail_node)

    t_req = now()
    n, t_end, kind, last = stream(base, on_token)
    rec.update(t_request=t_req, tokens_before_end=n, t_first_token=tokens[0] if tokens else None,
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
            rec.update(t_replace_try=re["t_place_try"], t_replace_cmd=re["t_place_cmd"], t_replaced=re["t_placed"], t_replace_ready=re["t_ready"])
            first = {}
            n2, t_end2, kind2, _ = stream(base, lambda i, t: first.setdefault("t", t), max_tokens=20)
            rec.update(t_resubmit_first_token=first.get("t"), resubmit_end_kind=kind2)
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
    print(json.dumps({k: rec[k] for k in ("mode", "tokens_before_end", "end_kind", "t_fail",
                                           "t_stream_end", "t_instance_gone", "t_replace_ready",
                                           "t_resubmit_first_token") if k in rec}, indent=1))


if __name__ == "__main__":
    sys.exit(main())
