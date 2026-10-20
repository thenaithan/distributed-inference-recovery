# Study notes for Experiment 4

What to read and in what order, and what we expect to find, written before any
measurement so that the expectations can be checked rather than fitted.

## The question

`distributed-inference-resilience` fits one constant to a 2023 paper: $C$, the cost of
one failure (detect it, find a replacement, rebuild state). Every threshold in that work
is $1/(S C)$ or $(1-p)^{ST}$ with a restart cost per failure. Experiment 4 measures $C$
and its parts on two systems people run today, exo and vLLM, under three ways of losing
a node, and replaces the fitted constant with a measured distribution.

A second question falls out of the source reading (FINDINGS-source.md): both modern
systems restart requests from scratch and restart nothing by themselves. So the
measurement also answers "what does a failure cost a request on a current stack",
which is the number the public conclusion's applicability section currently states
from documentation only.

## Three failure modes, and why all three

| mode | what it does | what it tests |
|---|---|---|
| `kill`  | SIGKILL the process / container | detection by connection reset: the fast path |
| `pause` | SIGSTOP it | detection by timeout: sockets stay open, nothing answers |
| `cut`   | disconnect from the network | detection by timeout plus discovery loss: the partition |

Experiment 2 found detection latency to be the largest term in $C$ under churn and
entirely client-side. `kill` and `pause` bracket it: the same node, gone either way,
and the system's own constants decide the difference (exo 5 s vs 90 s, vLLM immediate
vs 300 s, Petals one hop vs 180 s). A node that leaves a swarm without closing its
sockets, which is what a laptop going to sleep does, is the `pause` case.

## Predictions, from the source, to be checked

exo, worker killed at token 40 of a 3-node pipeline:

- detect: 5 s poll granularity, so 0 to 5 s after the ring error, which itself follows
  the next activation send (one token). Expect 1 to 6 s.
- gone: `InstanceDeleted` at 15 s node silence plus up to 1 s plan tick. Expect 15 to 17 s.
- recover: re-placement over two nodes, weights reloaded from disk for a 0.6B 8-bit
  model on CPU. Expect tens of seconds; this is the term we have no source number for.
- C: recover plus prefill of the prompt. Expect 30 to 90 s.

exo, worker paused: detect should jump to 90 s (`RUNNER_STALL_TIMEOUT`) unless the
ring layer's own TCP keepalive fires first; gone stays at ~15 s because the paused
node also stops its 1 s info reports. Which of the two the client sees first is a real
question: the master may delete the instance (and the API close the stream) before the
runner stall fires. If so, effective detection is ~15 s, not 90 s, and the comment in
FINDINGS-source.md needs correcting.

exo, master killed: expect zenoh expiry (≈10 s lease) then a 3 s election; every
stream ends with "generation interrupted" even though no shard was lost. That is a
coordination failure stopping computation that was fine, the Experiment 2 finding in
miniature.

vLLM on CPU, PP=2, worker killed: detect well under 1 s (sentinel); server exit at
+5 s (watchdog); recover = relaunch time, which for a 0.6B model on CPU should be
20 to 60 s, dominated by weight load and warm-up; C ≈ recover + prefill.

vLLM, worker paused: detect = 300 s, nothing else moves until then. If measured, this
is the single largest number in the study and the one to lead with.

## Reading, in order

1. FINDINGS-source.md in this repo. The mechanisms and constants, with file:line.
2. `distributed-inference-resilience/NOTES.md` §2 and §5, for what $C$ feeds and why
   detection dominates.
3. LUMEN, "Coordinated Failure Recovery for Distributed LLM Serving", arXiv:2606.17787
   (June 2026). The current framing of this exact problem for datacenter serving;
   their stop-and-restart baseline is what we are measuring, their checkpointing
   proposal is Petals' mechanism moved server-side.
4. DéjàVu (Strati et al., ICML 2024), the earlier KV-checkpointing design LUMEN compares to.
5. Llumnix (OSDI 2024), live migration of requests between vLLM instances; the
   mechanism a self-healing serving layer would need.
6. DistServe (OSDI 2024) and Splitwise (ISCA 2024) for why prefill/decode
   disaggregation exists, and Mooncake (FAST 2025) for a production KV-transfer
   layer; the P/D failure path in FINDINGS-source.md is theirs.
7. Chandra and Toueg (1996) and the φ accrual failure detector (2004), as before: the
   `pause` numbers are failure-detector numbers.
8. For exo's networking: zenoh's liveliness and lease model (zenoh.io docs, "liveliness"
   and "scouting"), and the bully election algorithm (Garcia-Molina 1982), which
   `election.py` implements with a seniority tie-break.
9. For the P2P side, Stutzbach and Rejaie (IMC 2006) again: measured session lengths
   decide whether a 300 s or 90 s detection timeout is survivable in practice.

## Running it

Both harnesses run on one Linux machine, no GPU, and write one JSON line per trial.
`analyze.py` reads the lines and prints the four times above plus the churnsim
parameters. Expect to run each (system, mode) ten times; the distributions matter more
than the medians because detection is bimodal.

The sandbox this was written in cannot run either system (no model downloads), so
neither harness has been executed yet. Run `--mode none` first on each to check the
plumbing before injecting failures.
