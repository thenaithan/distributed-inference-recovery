# distributed-inference-recovery

Experiment 4 of the distributed-inference resilience work: measure what one node
failure costs a request on systems people run today, and feed the measured constant
back into the model in
[distributed-inference-resilience](https://github.com/thenaithan/distributed-inference-resilience).

That model fits a single constant $C$ (detect a failure, find a replacement, rebuild
state) to the Petals paper. Here $C$ is measured on **exo** (consumer-device swarm) and
**vLLM** (datacenter serving, pipeline-parallel on CPU), under three failure modes:
a clean crash, a silent hang, and a network partition.

## Contents

- `FINDINGS-source.md` – what exo and vLLM do on node loss, read from source with
  file:line, and what it means for the model before any measurement. Start here.
- `STUDY.md` – predictions written before measuring, reading list, how to run.
- `exo/` – `Containerfile`, `cluster.sh` (N nodes in rootless Podman on an IPv6 network),
  `trial.py` (place, stream, inject failure, time recovery).
- `vllm/` – `trial.py` (serve with `-pp 2` on CPU, stream, kill or pause a worker, time
  relaunch).
- `analyze.py` – trial logs → detect / gone / recover / C, and churnsim parameters.

## Quick start

exo (needs Podman with IPv6 networking; first build downloads the model per node):

```
podman build -t exo-node -f exo/Containerfile exo
exo/cluster.sh up 3
python3 exo/trial.py --nodes 3 --mode none                     # plumbing check
python3 exo/trial.py --nodes 3 --fail-node 1 --mode kill  --at-token 40 --out exo/trials.jsonl
exo/cluster.sh up 3                                             # restart the killed node
python3 exo/trial.py --nodes 3 --fail-node 1 --mode pause --at-token 40 --out exo/trials.jsonl
python3 analyze.py exo/trials.jsonl
```

vLLM (a venv with `vllm` installed; CPU backend):

```
python3 vllm/trial.py --mode none
python3 vllm/trial.py --mode kill  --at-token 40 --out vllm/trials.jsonl
python3 vllm/trial.py --mode pause --at-token 40 --out vllm/trials.jsonl
python3 analyze.py vllm/trials.jsonl
```

## Status

- [x] failure paths and constants read from source (exo bc412e2, vLLM 8407bc0)
- [x] harnesses written; not yet run (the authoring environment cannot download models)
- [ ] plumbing check on both
- [ ] 10 trials per (system, mode)
- [ ] measured $C$ into the resilience model; update its applicability section
