# Study notes for Experiment 3

What to read and in what order, and what we expect to find, written before any
measurement so that the expectations can be checked rather than fitted.

## The question

`distributed-inference-resilience` fits one constant to a 2023 paper: $C$, the cost of
one failure (detect it, find a replacement, rebuild state). Every threshold in that work
is $1/(S C)$ or $(1-p)^{ST}$ with a restart cost per failure. Experiment 3 measures $C$
and its parts on systems people run today, exo and vLLM (and, added 2026-10-24 after the
vLLM trials, llama.cpp's RPC backend), under three ways of losing a node, and replaces the fitted constant with a measured distribution.

A second question falls out of the source reading (FINDINGS-source.md): all of these
systems restart requests from scratch and restart nothing by themselves. So the
measurement also answers "what does a failure cost a request on a current stack",
which is the number the public conclusion's applicability section currently states
from documentation only.

## The self-replicating AI article

Zhou and Freeman, "AI models may be capable of self-replicating without access to a
datacenter", Anthropic Frontier Red Team, 2026-10-12
(https://www.anthropic.com/research/self-replicating-ai). Read in full on 2026-10-21;
figures not stated in the article are marked **derived** here.

The article argues that small models may soon self-replicate across commercial IoT
devices, demonstrates Gemma4-26B-A4B pipeline-parallel over 15 Android TV boxes at
0.39 tok/s, and chooses pipeline parallelism "for its fault-tolerant properties",
citing Borzunov et al. 2022: a downed node can be replaced and its KV cache recomputed
by a forward pass of the preceding node. Its defence section frames the problem as
driving $R_0$ below 1 by quarantine.

Why this study bears on it:

- **Same mechanism, same source.** The fault-tolerance argument is Petals' Algorithm 1.
  The article cites the 2022 demo paper (arXiv:2209.01188); the fault-tolerance
  measurement is in the 2023 NeurIPS paper (arXiv:2312.08361), which is what the
  resilience model is fitted to.
- **Replacement is assumed, not costed.** The article measures neither $C$ nor churn.
  Its window times (27 to 32 h for the 26B model, derived) assume no box drops out for
  days, on devices that are unplugged and restarted daily. FINDINGS-source.md adds that
  today's stacks do not replace anything on their own; only Petals recovers in-request.
- **Rebuilding KV is a prefill, and on that hardware it is slow.** Derived from the
  article's App. C (a 32k window is ~28k prefill + 3.8 to 4.8k decode tokens, prefill
  ~15× faster per token, window 27.3 to 31.8 h): decode ≈ 15 to 20 s/token, prefill
  ≈ 1.0 to 1.35 s/token on the TV-box swarm. Recomputing KV for a late-window context
  is a prefill of up to 7.6 to 10.5 h inside a 27 to 32 h window. **Hypothesis**: in
  that regime $C$ is hours, and the $(1-p)^{ST}$ and $1/(SC)$ terms decide whether a
  window completes at all. This is what the measurement, restated in that regime,
  tests.
- **Quarantine is churn imposed by defenders.** With measured $C$ the model gives the
  removal rate at which a pipeline-parallel replica stops completing windows, a direct
  input to the article's $R_0$.

How our setup relates to theirs: short-context decode speed is similar (2.5 to 2.9
s/token here vs ≈ 2.6 there), prefill/decode ratio is not (≈ 1 here because MLX's CPU
loops do not amortise weight reads over a batch; ≈ 15 there), and our network is
near-ideal. So what carries over is counts: detection and orchestration in seconds
(timer-governed, hardware-independent), warmup and prefill in tokens, converted with
the article's per-token speeds. `analyze.py --regime` does the conversion and labels it
as an assumption.

**Scope.** Measure and model. Do not build the automatic re-placement that exo lacks;
that is the piece a replicator would need, and it is not ours to supply.

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

exo, worker killed after 5 output tokens of a 3-node pipeline (30 requested):

- detect: 5 s poll granularity, so 0 to 5 s after the ring error, which itself follows
  the next activation send (one token). Expect 1 to 6 s.
- gone: `InstanceDeleted` at 15 s node silence plus up to 1 s plan tick. Expect 15 to 17 s.
- replace_wait: placement is refused (400) until the master has dropped the node, so
  up to ~15 s after the failure, less whatever `gone` already consumed.
- recover: re-placement over two nodes. From the plumbing runs: load ~1 s, warmup
  ~150 s (50 decode tokens at ~2.9 s), so expect 150 to 200 s on this CPU; in tokens,
  50 decode tokens.
- prefill: ~70 s for the 29-token prompt here, i.e. ~2.5 s/token; in tokens, 29.
- C: 250 to 300 s on this CPU, dominated by warmup and prefill; restated in the
  article's regime (`--regime tvbox-26b`) the same trial is 13 to 17 min for this
  prompt, and a 28k-token prefill alone 7.6 to 10.5 h.

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

The harnesses run on one Linux machine, no GPU, and write one JSON line per trial.
`analyze.py` reads the lines and prints the four times above plus the churnsim
parameters. Expect to run each (system, mode) ten times; the distributions matter more
than the medians because detection is bimodal.

The authoring sandbox cannot run any of the systems (no model downloads), so harness changes
are checked against stub servers and synthetic records there and executed on the trial
machine. Run `--mode none` first after any harness change. The exo plumbing check
passed on 2026-10-21, vLLM's on 2026-10-23 and llama.cpp's on 2026-10-25 (README, Status).

## Addendum (2026-10-26): what the measurements did to the predictions

The predictions above are left as written. RESULTS.md §8 checks each one; the corrections,
in short:

- exo, worker lost: detection is not the 5 s poll or the 90 s stall detector but the
  master's 15 s node-silence clock, for kill, pause and cut alike (15.0 s, n=33). The
  placement refusal adds nothing, because the node is already dropped when the stream ends.
- exo, master lost: three detection paths, not one: 3.3 s (link close), 13.2 s (zenoh
  lease) or 30 to 36 s (heartbeat backstop); which one fires is not deterministic for kill.
- vLLM, worker killed: detect 1.5 s rather than well under 1 s, because the executor waits
  on the surviving rank's blocked send before it acts on the sentinel; relaunch 62 s, at
  the top of the 20 to 60 s range, from warm-up compilation on every start. Pause: 300.0 s,
  as predicted, and the largest number for a system that detects at all.
- llama.cpp RPC (predictions in FINDINGS-source.md, written from source on 2026-10-24
  before its trials): killproc as predicted (0.6 s, 13 s relaunch). The container kill was
  predicted to go undetected and was not: `podman kill` ends the process before the
  namespace, so the kernel closes the connection. Pause and cut are never detected, as
  predicted; a healed partition recovers only if the rpc host was mid-send when cut
  (`tcp_retries2`, about 950 s), otherwise the remote host must be restarted. Prefill is
  2.6 to 4.0 ms/token against 26 ms/token decode, a ratio of 7 to 10, which gives the
  regime restatement a measured constant between exo's ≈ 1 and the article's ≈ 15.
