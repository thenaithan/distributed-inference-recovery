#!/usr/bin/env python3
"""One failure trial against llama.cpp's RPC backend: measure C and its parts.

    python3 llamacpp/trial.py --mode kill --fail-rpc 1 --out llamacpp/trials.jsonl
    python3 llamacpp/trial.py --mode kill --prompt-tokens 2048 ...     # prefill vs length

Setup: `llamacpp/cluster.sh up 2` runs two ggml-rpc-server containers holding one layer
slice each and one llama-server container (the main host) that streams tokens. From
source (FINDINGS-source.md): the main host aborts the whole process on any RPC transport
error, has no socket timeout and no keepalive, and keeps a static peer list. So:

  kill      podman kills rpc-i's container (namespace torn down; peers see nothing)
  killproc  SIGKILL the rpc-server process inside its container (kernel resets the TCP
            connection; the main host's recv fails at once and it aborts)
  pause     SIGSTOP rpc-i: the main host blocks in recv; prediction: never detected
  cut       network disconnect: same prediction as pause
  none      plumbing check

Timestamps (one host clock): request, each token, injection, stream end, main-host
container exit, relaunch, ready (/health), resubmitted request, first token. Prefill is
reported by llama-server itself in `timings` (prompt_ms, prompt_n) when
`timings_per_token` is requested, so the per-token prefill cost is a measurement here,
not an inference from first-token latency.

The prompt is built to a requested token count by repeating a paragraph; the exact
count comes back in `usage.prompt_tokens`.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASE = "http://localhost:8080"
STALL_TIMEOUT = 600.0   # no token for this long -> harness gives up ("harness_timeout")
PARA = ("The river rises in the hills, gathers smaller streams, bends through farmland, "
        "slows across the plain, and reaches the sea by a wide delta. ")


def now() -> float:
    return time.time()


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def sh(*args) -> float:
    out = subprocess.run([str(HERE / "cluster.sh"), *map(str, args)], capture_output=True, text=True, check=True)
    lines = [l for l in out.stdout.strip().splitlines() if l.strip()]
    return float(lines[0]) if lines and lines[0].replace(".", "").isdigit() else now()


def healthy() -> bool:
    try:
        with urllib.request.urlopen(f"{BASE}/health", timeout=2) as r:
            return r.status == 200
    except Exception:
        return False


def main_running() -> bool:
    out = subprocess.run(["podman", "inspect", "-f", "{{.State.Running}}", "main"], capture_output=True, text=True)
    return out.returncode == 0 and out.stdout.strip() == "true"


def wait_ready(timeout: float = 900.0) -> float:
    t0 = now()
    while now() - t0 < timeout:
        if healthy():
            return now()
        if not main_running():
            tail = subprocess.run(["podman", "logs", "--tail", "25", "main"], capture_output=True, text=True)
            raise RuntimeError("main host exited during startup; its last log lines:\n"
                               + (tail.stdout + tail.stderr).rstrip())
        time.sleep(0.5)
    raise RuntimeError("main host not healthy within timeout")


def build_prompt(n_tokens: int) -> str:
    # ~28 tokens per PARA for Qwen's tokenizer; overshoot is fine, usage tells the truth
    reps = max(1, n_tokens // 28)
    return PARA * reps + "\nNow write a numbered list of 60 unrelated facts about rivers, one per line."


def stream(prompt: str, on_token, max_tokens: int, stall_timeout: float):
    # thinking off, as in exo/trial.py and vllm/trial.py: Qwen3 otherwise spends its first tokens
    # in reasoning_content and token --at-token is never reached
    body = {"model": "default", "stream": True, "max_tokens": max_tokens, "temperature": 0,
            "stream_options": {"include_usage": True}, "timings_per_token": True,
            "chat_template_kwargs": {"enable_thinking": False},
            "messages": [{"role": "user", "content": prompt}]}
    req = urllib.request.Request(f"{BASE}/v1/chat/completions", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    n, last, usage, timings = 0, None, {}, {}
    fin = None  # (kind, t) once a finish_reason arrives; the usage chunk follows it, then [DONE]
    t_progress = now()
    try:
        with urllib.request.urlopen(req, timeout=stall_timeout) as r:
            for raw in r:
                if now() - t_progress > stall_timeout:
                    return n, now(), "harness_timeout", last, usage, timings
                if not raw.startswith(b"data:"):
                    continue
                payload = raw[5:].strip()
                if payload == b"[DONE]":
                    return (n, fin[1], fin[0], last, usage, timings) if fin else (n, now(), "done", last, usage, timings)
                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                last = obj
                if obj.get("usage"):
                    usage = obj["usage"]
                if obj.get("timings"):
                    timings = obj["timings"]
                if "error" in obj:
                    return n, now(), "error_chunk", obj, usage, timings
                ch = obj.get("choices") or []
                delta = ch[0].get("delta", {}) if ch else {}
                if delta.get("content") or delta.get("reasoning_content"):
                    n += 1
                    t_progress = now()
                    on_token(n, t_progress)
                if ch and ch[0].get("finish_reason") and fin is None:
                    fin = (f"finish:{ch[0]['finish_reason']}", now())  # keep reading for usage
    except TimeoutError:
        return n, now(), "harness_timeout", last, usage, timings
    except Exception as e:  # connection reset when llama-server aborts
        return n, now(), f"exception:{type(e).__name__}", {"message": str(e)[:300]}, usage, timings
    return n, now(), "eof", last, usage, timings


def wait_main_exit(timeout: float) -> float | None:
    deadline = now() + timeout
    while now() < deadline:
        if not main_running():
            return now()
        time.sleep(0.2)
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["kill", "killproc", "pause", "cut", "none"], default="kill")
    ap.add_argument("--fail-rpc", type=int, default=1)
    ap.add_argument("--at-token", type=int, default=5)
    ap.add_argument("--max-tokens", type=int, default=30)
    ap.add_argument("--prompt-tokens", type=int, default=33)
    ap.add_argument("--stall-timeout", type=float, default=STALL_TIMEOUT)
    ap.add_argument("--out", default="llamacpp/trials.jsonl")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    rec: dict = dict(label=args.label, system="llamacpp-rpc", mode=args.mode, fail_rpc=args.fail_rpc,
                     at_token=args.at_token, max_tokens=args.max_tokens, prompt_tokens_requested=args.prompt_tokens,
                     stall_timeout=args.stall_timeout, host_time=now())
    prompt = build_prompt(args.prompt_tokens)

    if not healthy():
        log("main host not healthy; starting it")
        rec["t_launch"] = sh("main")
        rec["t_ready"] = wait_ready()
        log(f"ready after {rec['t_ready'] - rec['t_launch']:.0f}s")

    tokens: list = []
    state = {"t_fail": None}

    def on_token(i, t):
        tokens.append(t)
        if args.mode != "none" and i == args.at_token and state["t_fail"] is None:
            state["t_fail"] = sh(args.mode, args.fail_rpc)
            log(f"injected {args.mode} on rpc-{args.fail_rpc} at token {i}")
        if i == 1:
            log(f"first token {t - t_req:.1f}s after the request")

    t_req = now()
    n, t_end, kind, last, usage, timings = stream(prompt, on_token, args.max_tokens, args.stall_timeout)
    rec.update(t_request=t_req, t_first_token=tokens[0] if tokens else None, tokens_before_end=n,
               t_fail=state["t_fail"], t_stream_end=t_end, end_kind=kind, usage=usage, timings=timings,
               inter_token_s=[round(b - a, 4) for a, b in zip(tokens, tokens[1:])], last_payload=last)
    log(f"stream ended: {kind} after {n} tokens")
    if args.mode == "none" and (n == 0 or not (usage or {}).get("prompt_tokens") or not (timings or {}).get("prompt_ms")):
        with open(args.out, "a") as f:
            f.write(json.dumps(rec) + "\n")
        raise SystemExit(f"plumbing check FAILED: {n} tokens, usage={usage}, timings={timings} "
                         f"(record appended to {args.out})")

    if args.mode != "none":
        # the main host aborts on a transport error; a hang leaves it blocked. Either way,
        # recovery = restart the main host (the rpc host too, if it is gone) and resubmit.
        rec["t_main_exit"] = wait_main_exit(timeout=60.0)
        if args.mode == "pause":
            sh("resume", args.fail_rpc)
        elif args.mode == "cut":
            sh("heal", args.fail_rpc)
        elif args.mode in ("kill", "killproc"):
            # bring the rpc host back; cluster.sh up recreates all rpc hosts and the main host
            pass
        try:
            if args.mode in ("kill", "killproc"):
                subprocess.run([str(HERE / "cluster.sh"), "up"], capture_output=True, text=True, check=True)
                rec["t_relaunch"] = now()
            else:
                rec["t_relaunch"] = sh("main")
            rec["t_ready_again"] = wait_ready()
            first = {}
            t_resub = now()
            n2, t_end2, kind2, _, usage2, timings2 = stream(prompt, lambda i, t: first.setdefault("t", t),
                                                           max_tokens=5, stall_timeout=args.stall_timeout)
            rec.update(t_resubmit_request=t_resub, t_resubmit_first_token=first.get("t"),
                       resubmit_end_kind=kind2, resubmit_usage=usage2, resubmit_timings=timings2)
        except Exception as e:
            rec["recover_error"] = f"{type(e).__name__}: {e}"

    with open(args.out, "a") as f:
        f.write(json.dumps(rec) + "\n")
    keys = ("mode", "tokens_before_end", "end_kind", "t_fail", "t_stream_end", "t_main_exit",
            "t_ready_again", "t_resubmit_first_token")
    summary = {k: rec.get(k) for k in keys}
    summary["prompt_tokens"] = (usage or {}).get("prompt_tokens")
    summary["prompt_ms"] = (timings or {}).get("prompt_ms")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    sys.exit(main())
