#!/usr/bin/env python3
"""One failure trial against vLLM with pipeline parallelism on CPU: measure C and its parts.

Setup (once, in a venv on the host; no GPU needed, PP works on the CPU backend). The
release CPU wheel (checked with v0.32.0; the docs' URL says manylinux_2_34 but the asset
is manylinux_2_39) and the two environment variables a one-socket desktop needs:

    V=0.32.0
    pip install "https://github.com/vllm-project/vllm/releases/download/v$V/vllm-$V+cpu-cp38-abi3-manylinux_2_39_x86_64.whl" \
        --extra-index-url https://download.pytorch.org/whl/cpu
    # the wheel's metadata says `triton==3.8.0`, which pip resolves to the NVIDIA triton; torch
    # inductor then asks the CUDA driver during the warm-up compile and the worker dies with
    # "Torch not compiled with CUDA enabled". vLLM's own requirements/cpu.txt wants triton-cpu:
    pip uninstall -y triton
    pip install "triton @ https://github.com/triton-lang/triton-cpu/releases/download/v3.8.0/triton-3.8.0+cpu-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl"
    # triton-cpu still ships the NVIDIA backend and prefers it whenever libcuda.so.1 loads and
    # cuInit succeeds (triton/runtime/driver.py), i.e. on any host with an NVIDIA driver, and
    # then dies the same way because torch is the CPU build. trial.py exports this itself:
    export TRITON_DEFAULT_BACKEND=cpu
    export LD_PRELOAD="$(find "$VIRTUAL_ENV" -name libiomp5.so | head -1):$LD_PRELOAD"
    export VLLM_CPU_OMP_THREADS_BIND="0-9|10-19"   # 'auto' wants one NUMA node per rank and aborts with one
    python3 trial.py --serve    # launches `vllm serve MODEL -pp 2 --port 8000`, waits for /health

On the CPU backend `--gpu-memory-utilization` is the fraction of *RAM* that must be free at
start (default 0.92, so a busy host fails with "Available memory on node 0 ... is less
than desired"); VLLM_MEM_UTIL (default 0.3) sets it. KV cache per rank is vLLM's CPU
default (4 GiB) unless VLLM_CPU_KVCACHE_SPACE is set.

Trial:

    python3 trial.py --mode kill  --at-token 40 --out trials.jsonl
    python3 trial.py --mode pause --at-token 40 --out trials.jsonl     # SIGSTOP: the hang case

What happens, timestamped with one host clock:

  1. find the server's worker processes (children of the engine core; two for -pp 2)
  2. start a streaming completion; timestamp every token
  3. at token --at-token: SIGKILL (kill) or SIGSTOP (pause) worker --fail-rank
  4. read the stream until it ends (HTTP 500 / connection drop); timestamp
  5. poll /health until it fails and until the server process is gone; timestamp both
  6. relaunch `vllm serve`, wait for /health, resubmit the prompt, timestamp first token

So: detect = t_stream_end - t_fail (what the client sees), server_dead = t_server_exit - t_fail,
recover = t_ready_again - t_server_exit (orchestrator relaunch, here done by this script),
total C = t_resubmit_first_token - t_fail. Records one JSON line per trial.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request

MODEL = os.environ.get("VLLM_MODEL", "Qwen/Qwen3-0.6B")
PORT = int(os.environ.get("VLLM_PORT", "8000"))
BASE = f"http://localhost:{PORT}"
PROMPT = "Write a long, detailed, numbered list of 60 unrelated facts about rivers, one per line."
# OpenMP thread binding per PP rank. Default "auto" is vLLM's own. When exo's nodes are
# pinned (exo/cluster.sh EXO_CPUSETS), set the same sets here, e.g. "0-3|4-7", so both
# systems get equal cores. A server passed with --server-pid must be launched with it too.
CPU_BIND = os.environ.get("VLLM_CPU_OMP_THREADS_BIND", "auto")
MEM_UTIL = os.environ.get("VLLM_MEM_UTIL", "0.3")  # fraction of RAM that must be free at start (CPU backend)
SERVE_CMD = ["vllm", "serve", MODEL, "--pipeline-parallel-size", "2", "--port", str(PORT),
             "--max-model-len", "2048", "--gpu-memory-utilization", MEM_UTIL]


def now() -> float:
    return time.time()


def healthy(timeout=2.0) -> bool:
    try:
        with urllib.request.urlopen(f"{BASE}/health", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def launch_server() -> tuple[subprocess.Popen, float, float]:
    if healthy():
        raise RuntimeError(f"something already answers {BASE}/health; stop it (pkill -f 'vllm serve') "
                           "or pass --server-pid")
    t0 = now()
    env = dict(os.environ, VLLM_LOGGING_LEVEL=os.environ.get("VLLM_LOGGING_LEVEL", "INFO"),
               VLLM_CPU_OMP_THREADS_BIND=CPU_BIND,
               # triton-cpu picks the NVIDIA backend on a host with an NVIDIA driver (see docstring)
               TRITON_DEFAULT_BACKEND=os.environ.get("TRITON_DEFAULT_BACKEND", "cpu"))
    proc = subprocess.Popen(SERVE_CMD, env=env, stdout=open("vllm-serve.log", "ab"), stderr=subprocess.STDOUT)
    while not healthy():
        if proc.poll() is not None:
            raise RuntimeError("vllm serve exited during startup; see vllm-serve.log")
        time.sleep(0.5)
    return proc, t0, now()


def worker_pids(server_pid: int) -> list[int]:
    """Worker processes = descendants of the serve process whose cmdline marks them as
    vLLM workers. The engine core is one child; its children are the PP ranks."""
    out = subprocess.run(["ps", "-eo", "pid,ppid,args"], capture_output=True, text=True).stdout
    rows = [l.split(None, 2) for l in out.strip().splitlines()[1:]]
    children = {}
    for pid, ppid, args in rows:
        children.setdefault(int(ppid), []).append((int(pid), args))
    found = []
    def walk(p):
        for pid, args in children.get(p, []):
            if "VLLM" in args.upper() and ("Worker" in args or "worker" in args):
                found.append(pid)
            walk(pid)
    walk(server_pid)
    return sorted(found)


def stream(on_token, max_tokens=400):
    # thinking off, as in exo/trial.py, so both systems prefill the same templated prompt
    body = {"model": MODEL, "stream": True, "max_tokens": max_tokens, "temperature": 0,
            "chat_template_kwargs": {"enable_thinking": False},
            "messages": [{"role": "user", "content": PROMPT}]}
    req = urllib.request.Request(f"{BASE}/v1/chat/completions", data=json.dumps(body).encode(),
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
                if "error" in obj:
                    return n, now(), "error_chunk", obj
                ch = obj.get("choices") or []
                delta = ch[0].get("delta", {}) if ch else {}
                if delta.get("content") or delta.get("reasoning_content"):
                    n += 1
                    on_token(n, now())
                if ch and ch[0].get("finish_reason"):
                    return n, now(), f"finish:{ch[0]['finish_reason']}", obj
    except Exception as e:
        return n, now(), f"exception:{type(e).__name__}", {"message": str(e)[:300]}
    return n, now(), "eof", last


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true", help="just launch the server and exit")
    ap.add_argument("--server-pid", type=int, help="pid of a running `vllm serve` (else launch one)")
    ap.add_argument("--mode", choices=["kill", "pause", "none"], default="kill")
    ap.add_argument("--fail-rank", type=int, default=1, help="index into the sorted worker pids")
    ap.add_argument("--at-token", type=int, default=40)
    ap.add_argument("--out", default="trials.jsonl")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    rec: dict = dict(label=args.label, model=MODEL, mode=args.mode, at_token=args.at_token,
                     fail_rank=args.fail_rank, host_time=now(), pp=2, omp_threads_bind=CPU_BIND,
                     mem_util=MEM_UTIL)

    if args.server_pid:
        proc, spid = None, args.server_pid
    else:
        proc, t0, t_ready = launch_server()
        spid = proc.pid
        rec.update(t_launch=t0, t_ready=t_ready, startup_s=round(t_ready - t0, 2))
        if args.serve:
            print(json.dumps(rec)); print(f"server pid {spid}; leave it running")
            return

    pids = worker_pids(spid)
    rec["worker_pids"] = pids
    if args.mode != "none" and len(pids) <= args.fail_rank:
        raise SystemExit(f"found worker pids {pids}; cannot fail rank {args.fail_rank}")

    tokens: list = []
    state = {"t_fail": None}

    def on_token(i, t):
        tokens.append(t)
        if args.mode != "none" and i == args.at_token and state["t_fail"] is None:
            sig = signal.SIGKILL if args.mode == "kill" else signal.SIGSTOP
            state["t_fail"] = now()
            os.kill(pids[args.fail_rank], sig)

    t_req = now()
    n, t_end, kind, last = stream(on_token)
    rec.update(t_request=t_req, t_first_token=tokens[0] if tokens else None, tokens_before_end=n,
               t_fail=state["t_fail"], t_stream_end=t_end, end_kind=kind,
               inter_token_s=[round(b - a, 4) for a, b in zip(tokens, tokens[1:])], last_payload=last)

    if args.mode != "none":
        # health flips and the server exits (watchdog) unless VLLM_KEEP_ALIVE_ON_ENGINE_DEATH
        t_unhealthy = None
        deadline = now() + 400
        while now() < deadline and healthy(1.0):
            time.sleep(0.2)
        t_unhealthy = now()
        rec["t_unhealthy"] = t_unhealthy
        if proc is not None:
            try:
                proc.wait(timeout=400)
                rec["t_server_exit"] = now()
            except subprocess.TimeoutExpired:
                rec["t_server_exit"] = None
        if args.mode == "pause":
            try:
                os.kill(pids[args.fail_rank], signal.SIGCONT)
            except ProcessLookupError:
                pass
        # relaunch, as an orchestrator would, and resubmit
        try:
            proc2, t_l, t_r = launch_server()
            rec.update(t_relaunch=t_l, t_ready_again=t_r)
            first = {}
            stream(lambda i, t: first.setdefault("t", t), max_tokens=20)
            rec["t_resubmit_first_token"] = first.get("t")
            proc2.terminate()
        except Exception as e:
            rec["recover_error"] = f"{type(e).__name__}: {e}"

    if proc is not None and args.mode == "none":
        proc.terminate()  # the server we launched; failure modes end with their own relaunch

    with open(args.out, "a") as f:
        f.write(json.dumps(rec) + "\n")
    keys = ("mode", "tokens_before_end", "end_kind", "t_fail", "t_stream_end", "t_unhealthy",
            "t_server_exit", "t_ready_again", "t_resubmit_first_token")
    print(json.dumps({k: rec.get(k) for k in keys}, indent=1))


if __name__ == "__main__":
    sys.exit(main())
