# distributed-inference-recovery

Experiment 3 of the distributed-inference resilience work: measure what one node
failure costs a request on systems people run today, and feed the measured constant
back into the model in
[distributed-inference-resilience](https://github.com/thenaithan/distributed-inference-resilience).

That model fits a single constant $C$ (detect a failure, find a replacement, rebuild
state) to the Petals paper. Here $C$ is measured on **exo** (consumer-device swarm),
**vLLM** (datacenter serving, pipeline-parallel on CPU) and **llama.cpp's RPC backend**
(the CPU stack most likely to be found on real consumer devices), under three failure
modes: a clean crash, a silent hang, and a network partition.

## Contents

- `FINDINGS-source.md` – what exo, vLLM and llama.cpp RPC do on node loss, read from source with
  file:line, and what it means for the model before any measurement. Start here.
- `STUDY.md` – framing (incl. the Frontier Red Team self-replication article),
  predictions written before measuring, reading list, how to run.
- `exo/` – `Containerfile`, `cluster.sh` (N nodes in rootless Podman on an IPv6 network),
  `trial.py` (place, stream, inject failure, time recovery).
- `vllm/` – `trial.py` (serve with `-pp 2` on CPU, stream, kill or pause a worker, time
  relaunch).
- `llamacpp/` – `Containerfile`, `cluster.sh` (N `ggml-rpc-server` hosts + one
  `llama-server` main host on a podman network), `trial.py` (kill / killproc / pause /
  cut an rpc host; `--prompt-tokens` sweeps prefill length).
- `RESULTS.md` – measured numbers for all three systems, and the predictions checked against them (§8).
- `analyze.py` – trial logs → detect / gone / recover / C, and churnsim parameters.

## Quick start

CPU pinning is off by default: exo's nodes float over all host CPUs and vLLM uses its own
`auto` thread binding. To give each node or PP rank its own equal cores, set both
`EXO_CPUSETS` (exo, one set per node, e.g. `"0-3 4-7 8-11"`) and `VLLM_CPU_OMP_THREADS_BIND`
(vLLM, e.g. `"0-3|4-7"`). exo's pinning needs the `cpuset` cgroup controller delegated to
your user (a systemd change and a re-login); `cluster.sh up` prints how if it is missing.
Each trial records the CPU sets or binding it ran with. The llama.cpp harness has no pinning;
each host runs `LLAMA_THREADS` threads (default 4).

exo (needs Podman with IPv6 networking; first build downloads the model per node):

```
podman build -t exo-node -f exo/Containerfile exo
exo/cluster.sh up 3
python3 exo/trial.py --nodes 3 --mode none                     # plumbing check
python3 exo/trial.py --nodes 3 --fail-worker --mode kill  --out exo/trials.jsonl   # a non-master node; injects after 5 tokens, 30 requested
exo/cluster.sh down && exo/cluster.sh up 3                      # fresh cluster between trials (restores the failed node)
python3 exo/trial.py --nodes 3 --fail-worker --mode pause --out exo/trials.jsonl
python3 exo/trial.py --nodes 3 --fail-master --mode kill --out exo/trials.jsonl       # master loss: kills whichever node is master, streams via a survivor
nohup exo/run_trials.sh cut worker 10 > exo/logs/run_trials.out 2>&1 &            # unattended: recreate cluster + trial until 10 complete cut-worker results
python3 analyze.py exo/trials.jsonl --regime tvbox-26b
```

vLLM (CPU backend; install steps in the `vllm/trial.py` docstring; the harness sets the
OpenMP binding, preload and triton backend itself):

```
source .venv/bin/activate
python3 vllm/trial.py --mode none
python3 vllm/trial.py --mode kill  --at-token 5 --out vllm/trials.jsonl
python3 vllm/trial.py --mode pause --at-token 5 --out vllm/trials.jsonl
for i in $(seq 10); do python3 vllm/trial.py --mode kill --at-token 5 --out vllm/trials.jsonl; done   # each trial launches and stops its own server
python3 analyze.py vllm/trials.jsonl
```

llama.cpp (rootless Podman; first start downloads `ggml-org/Qwen3-0.6B-GGUF` into the
`llama-models` volume):

```
podman build -t llamacpp-rpc -f llamacpp/Containerfile llamacpp
llamacpp/cluster.sh up 2
python3 llamacpp/trial.py --mode none --out llamacpp/smoke.jsonl                      # plumbing: usage.prompt_tokens and timings.prompt_ms present?
python3 llamacpp/trial.py --mode killproc --out llamacpp/trials.jsonl                 # reset: expect sub-second detect, then relaunch
python3 llamacpp/trial.py --mode kill     --out llamacpp/trials.jsonl                 # container killed: measured as the same reset as killproc (RESULTS §5)
python3 llamacpp/trial.py --mode pause    --out llamacpp/trials.jsonl                 # hang: never detected; the harness gives up at 600 s
nohup llamacpp/run_trials.sh all > llamacpp/logs/run_trials.out 2>&1 &              # unattended: 10 × killproc, 5 × kill/pause/cut, killproc at 512/2048/6144 prompt tokens ×2 (~4 h)
python3 analyze.py llamacpp/trials.jsonl
```

## Status

- [x] failure paths and constants read from source (exo bc412e2, vLLM 8407bc0, llama.cpp 74bca39)
- [x] harnesses written
- [x] exo plumbing check (2026-10-21); image fixes for exo's mlx-cpu pairing and MLX's JIT path
- [x] reported upstream: exo placement deadlock [exo#2464](https://github.com/exo-explore/exo/issues/2464), mlx-cpu pairing in [exo#2359](https://github.com/exo-explore/exo/issues/2359) (2026-10-23)
- [x] vLLM plumbing check (2026-10-23); CPU install needed four fixes, documented in `vllm/trial.py`
- [x] 10 trials per (system, mode) (exo 2026-10-21..23: kill/pause/cut worker, kill master ×10, pause/cut master ×5; vLLM 2026-10-24/25: kill, pause ×10)
- [x] llama.cpp (2026-10-25/26): plumbing check, killproc ×10, kill / pause / cut ×5, prefill sweep at 571 / 2221 / 6601 prompt tokens ×2
- [x] all three systems analysed: `RESULTS.md` (llama.cpp in §5, predictions checked in §8)
- [ ] measured $C$ into the resilience model; update its applicability section
