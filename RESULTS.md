# Results

What one node failure costs a request, measured on exo, vLLM and llama.cpp's RPC backend.
Every number here is reproduced by `python3 analyze.py exo/trials.jsonl`, `python3
analyze.py vllm/trials.jsonl` and `python3 analyze.py llamacpp/trials.jsonl` from the
committed records. Medians with min–max and n. Times are
seconds after the injected failure unless stated.

Setup (NOTES-2026-10-21.md §7): i5-14600K, 20 logical CPUs unpinned, rootless Podman;
exo bc412e2 with the image deviations noted there, `mlx-community/Qwen3-0.6B-8bit`,
3 nodes, pipeline sharding; vLLM 0.32.0 CPU wheel, `Qwen/Qwen3-0.6B`, PP=2; llama.cpp
74bca39, `ggml-org/Qwen3-0.6B-GGUF` Q8_0, two `ggml-rpc-server` hosts holding one layer
slice each and a `llama-server` main host with no layers, 4 threads per host. One
request, failure injected after 5 output tokens of 30, prompt of 33 tokens (61 after the
chat template). exo trials 2026-10-21 to 23 (53), vLLM 2026-10-24/25 (20), llama.cpp
2026-10-25 (32 valid, §5).

## 1. Summary table

| | exo, worker lost | exo, master lost | vLLM, PP worker killed | vLLM, PP worker hung | llama.cpp, rpc host killed | llama.cpp, rpc host hung or cut |
|---|---|---|---|---|---|---|
| detect (client learns the request is dead) | **15.0 s** (14.3–16.0, n=33) | 3.3 / 13.2 / ~31 s, which one is not deterministic (n=20) | **1.5 s** (0.0–3.7, n=10) | **300.0 s** (300.0–300.1, n=10) | **0.5 s** (0.5–0.8, n=13) | **never** (harness gave up at 600 s, n=10) |
| gone (instance deleted / server exited) | 15.0 s | 28 or 46 s | 4.5 s (4.0–8.6) | 305 s | 0.7 s | never |
| re-place or relaunch to serving | 174 s (173–180): load 0.6 s, warmup 168 s | 174 s | 62 s (59–75) | 75 s (63–96) | 13 s (recreate 3 containers) | 2 s, if the remote host lets go (§5) |
| prefill of the 33-token prompt | 85 s | 85 s | 0.3 s | 0.3 s | 0.2 s | 0.2 s |
| **C** (failure → first token of the resubmitted request) | **275 s** (272–282) | **305 s** (277–310), kill; 301 s pause; 288 s cut | **66.6 s** (64–85) | **373 s** (369–403, n=6; 4 trials are harness artefacts, see §4) | **14 s** (13.8–14.9) | pause: 600 s + 3 s; cut: **952 s** when it recovers at all |
| recoveries that never complete | 2 of 53 trials (placement deadlock) | | 0 | 0 | 0 | cut: 4 of 7 without operator action |
| strategy | restart | restart | restart | restart | restart | restart (by hand) |
| steady decode | 2.40 s/token | | 0.06 s/token | | 0.026 s/token | |

No system preserved anything of the interrupted request; every trial ended with an
error on the stream, or with the harness giving up on a stream that never ended, and a
fresh request after recovery. That was predicted from source (FINDINGS-source.md) and
holds without exception in 105 trials.

## 2. exo: a lost worker is one case, decided by the master's clock

Kill (SIGKILL the container), pause (SIGSTOP) and cut (`podman network disconnect`,
sockets left open) were run 10 to 12 times each on a non-master node. The stream ended
14.3 to 16.0 s after the failure in all 33, by the master emitting `InstanceDeleted`
after `NODE_SILENCE_TIMEOUT` (15 s) and the API converting that into an error chunk.
The shape of the failure made no difference:

| mode | detect | C |
|---|---|---|
| kill (n=12, 11 recovered) | 15.0 (14.3–15.4) | 274.9 (272.4–282.4) |
| pause (n=11, 10 recovered) | 15.2 (14.5–15.9) | 274.5 (273.1–278.2) |
| cut (n=10) | 15.1 (14.4–16.0) | 273.7 (273.0–281.2) |

Two predicted detectors never fired. The ring-error path (surviving rank's send fails,
supervisor notices at its 5 s poll; predicted 1 to 6 s for kill) did not appear in any
trial: a killed container's packets are dropped rather than reset, so the survivor's
send blocks. The 90 s runner stall detector (predicted for pause) was never reached
because the master's 15 s clock always fires first; it matters only for a runner that
hangs while its node keeps reporting. Detection in exo is therefore a single timer, and
it is the master's.

## 3. exo: a lost master is detected by one of three paths

Twenty master losses (10 kill, 5 pause, 5 cut; the harness streams via a surviving
node's API). Detection took one of three values, and for kill which one is not
deterministic:

| path | detect | mechanism | kill | pause | cut |
|---|---|---|---|---|---|
| link close | 3.3 s | dying container closes TCP, zenoh liveliness token deleted within 0.1 s, election 0.2 s later, 3 s round | 3 | 0 | 0 |
| zenoh lease | 13.2 s | no close reaches peers; zenoh's default 10 s link lease expires, then the election | 0 | 3 | 5 |
| heartbeat | 30.2–36.0 s | `MASTER_SILENCE_TIMEOUT` 30 s, then the election | 7 | 2 | 0 |

`gone` follows at detect + 15 s: the new master runs its own silence clock on the dead
node before deleting the instance. C is 277 to 310 s, the same recovery as a worker
loss plus the longer detection. In every master-loss trial the request died although no
shard had been lost: the master change drops all generation tasks. That is a
coordination failure stopping intact computation, the Experiment 2 finding in miniature.

## 4. Recovery cost

**exo.** Re-placement is accepted first try (`replace_wait` 0.0 s in all 46 recoveries:
by the time the instance is deleted the dead node is already out of the topology). Load
is 0.5 to 0.9 s (weights on disk). Warmup is 168 s (167.5–177.1): exo decodes 50 tokens
before `RunnerReady`, at 3.4 s/token, slower than the 2.4 s/token steady decode. Prefill
of the 33-token prompt is 85 s (84.6–88.8), 2.6 s/token, the same per-token cost as
decode, because MLX's CPU backend does not amortise weight reads over a batch. On this
hardware C is 92% warmup plus prefill and 5% detection. In tokens: 50 decode tokens of
warmup plus the prompt's prefill.

**vLLM.** Relaunch is 62 s (59–75) after a kill and 75 s (63–96) after a hang; the
warm-up compile runs on every start even with the torch-compile cache warm. Prefill is
0.3 s. C after a kill is 66.6 s (64.1–84.6), 93% relaunch.

**The tail (exo).** Two of 53 trials wedged on re-placement: both runners stuck at
`RunnerIdle`, no timeout in exo (`initialize_timeout` is dead code), the cluster
recreated by hand. With the two wedges in the plumbing runs that is 4 in about 110
placements. C for exo has a mass at infinity of 2 to 4% that no median shows; filed as
exo#2464.

**vLLM pause artefact.** In 4 of 10 hang trials the API server process outlived its
closed port by more than 400 s; the harness waited for the process, so those records
carry C of 775 to 785 s. That is a harness measurement of a vLLM shutdown quirk
(`kill_process_tree` enumerating children after the engine core has already exited),
not a recovery time; the six clean trials give C = 373 s (368.7–402.8). Reported as the
range in §1.

## 5. llama.cpp RPC: sub-second on a reset, never otherwise

| mode (n) | detect | main host exits | relaunch to serving | C |
|---|---|---|---|---|
| killproc, SIGKILL the rpc-server process (8) | 0.6 s (0.5–0.8) | 0.8 s | 13 s | 14.2 s (13.8–14.9) |
| kill, `podman kill` the container (5) | 0.5 s (0.5–0.6) | 0.7 s | 13 s | 13.9 s (13.8–14.5) |
| pause, SIGSTOP (5) | never (600 s) | never | 2 s (main host only) | 600 s of harness patience + 3 s |
| cut, network disconnect (5 complete, 7 attempts) | never (600 s) | never | 283 s (144–624) after the heal | 952 s (760–1240) |

Kill and killproc are the same case, against the prediction: `podman kill` terminates the
process before its namespace goes, so the kernel still closes the TCP connection and the
main host's `recv` fails. llama.cpp then does exactly what its source says: `GGML_ABORT`,
the whole `llama-server` process gone within a second. Recovery is the operator's: recreate
the containers (13 s here, 2 s of which is the server coming up), resubmit.

Pause and cut are the same case on the detection side: nothing in llama.cpp RPC has a
timeout or a keepalive, so the main host blocks in `recv` indefinitely and the client sees
a stream that never ends. The two differ in recovery. After a pause, resuming the host and
restarting the main host is enough (2 s). After a partition, healing the network is not: the
rpc server serves one client at a time and is still inside `rpc_serve_client` on the vanished
main host's connection, so the restarted main host connects and waits forever for its hello
to be answered. The rpc host frees itself in exactly one case: if it was mid-send when cut,
the kernel's retransmissions run out (`tcp_retries2` = 15, documented lower bound 924.6 s)
and the send fails. Three trials recovered at 950, 952 and 954 s after the cut, the rpc
host's own log dating the close to the second; two hung for 1500 s and two more were
recreated by the harness after a 600 s restart window. Whether a healed partition heals
llama.cpp RPC is decided by the pipeline phase at the instant of the cut, roughly a coin
flip, and when it does, it takes 15 minutes.

**Prefill as a function of prompt length** (killproc, n=2 per length; prefill as reported by
`llama-server` on the fresh main host after the relaunch; the first stream of a trial hits
the prompt cache left by the previous trial's resubmit, so it is not used):

| actual prompt tokens | prefill | per token | C |
|---|---|---|---|
| 61 | 0.2 s | 3.3 ms | 14.2 s |
| 571 | 1.5 s | 2.6 ms | 15.6 s |
| 2221 | 6.0 s | 2.7 ms | 19.9 s |
| 6601 | 26.5 s | 4.0 ms | 40.4 s |

Decode is 26 ms per token, so the prefill/decode ratio on this CPU is 7 to 10 with a
batched, optimised backend. exo's MLX backend gave about 1 (prefill as slow per token as
decode); the Frontier Red Team article's derived figures imply about 15. The regime
conversion in §7 can now rest on a measured constant.

## 6. Detection, across systems

| | crash | hang | partition |
|---|---|---|---|
| Petals (from source, NOTES §4 of the model) | one hop | 180 s | 180 s |
| exo, worker | 15 s | 15 s | 15 s |
| exo, master | 3.3 or 30 s | 13.2 or 30 s | 13.2 s |
| vLLM | 1.5 s | 300 s | not measured |
| llama.cpp RPC | 0.5 s | never | never |

The prediction that detection is bimodal (fast on a crash, slow on a hang) held for
vLLM: 1.5 s against 300 s, a factor of 200, entirely `VLLM_EXECUTE_MODEL_TIMEOUT_SECONDS`.
It did not hold for exo's workers, where a single 15 s clock covers every shape, and it
held only partially for exo's master, where the fast path is taken by chance. vLLM's
1.5 s is not quite the process sentinel: the sentinel fires in the same second, but the
executor then waits for the surviving rank to come out of its blocked send to the dead
peer, 0 s if it was between steps and up to 3.7 s if mid-send.

## 7. Parameters for the resilience model

All three systems are the `restart` strategy of Experiment 1 (the one that collapses once a
generation expects about one failure, $p \approx 1/(ST)$), and neither re-places or
relaunches anything by itself: `join_rate` is whatever an operator or orchestrator
supplies, and for a swarm nobody operates it is zero.

| parameter | exo (worker) | vLLM (kill) | vLLM (hang) | llama.cpp (kill) | llama.cpp (hang, cut) |
|---|---|---|---|---|---|
| `detect_time` | 15.0 s | 1.5 s | 300 s | 0.5 s | ∞ (client timeout) |
| `reconnect_time` (C minus prefill) | 190 s on this CPU (50 warmup tokens + 15 s + load) | 66 s | 373 s | 14 s | ∞, or ~950 s with probability ~½ after a partition |
| recovery never completes | 2–4% | 0 | 0 | 0 | cut: ~½ without an operator |

Restated in the Frontier Red Team article's regime (`analyze.py --regime tvbox-26b`;
the measured timers kept, warmup and prefill converted with the article's derived
per-token speeds; an assumption, not a measurement): the exo trial is 13 to 18 minutes
for a 33-token prompt, and a 28k-token prefill alone is 7.8 to 10.5 hours. The
conversion rested on a prefill/decode ratio not measured on CPU (exo's is ≈ 1, the
article's ≈ 15); llama.cpp's measured ratio is 7 to 10 (§5), between the two, and is the
constant to use for a batched CPU backend.

## 8. Predictions against measurements

| prediction (STUDY.md) | measured | verdict |
|---|---|---|
| exo worker kill detect 1–6 s | 15 s | wrong: the ring error never surfaces; the master's clock decides |
| exo worker pause detect 90 s, or ~15 s if the master's deletion wins | 15 s | the second branch |
| exo `gone` 15–17 s | 15 s | right |
| exo recover 150–200 s, load ~1 s, warmup ~150 s | 174 s, 0.6 s, 168 s | right |
| exo C 250–300 s | 275 s | right |
| exo master: zenoh expiry ~10 s + 3 s election, or 30 s | 3.3 / 13.2 / 30 s, by path | right, with a third path |
| vLLM kill detect < 1 s | 1.5 s (0–3.7) | nearly: the executor waits on the blocked survivor |
| vLLM kill relaunch 20–60 s | 62 s | slightly over: warm-up compile on every start |
| vLLM pause detect 300 s | 300.0 s | right |
| placement refusal adds up to 15 s to recovery | 0 s | wrong: the node is already dropped when deletion fires |
| llama.cpp killproc detect well under 1 s, relaunch seconds | 0.6 s, 13 s | right |
| llama.cpp kill (container torn down) never detected | 0.5 s | wrong: `podman kill` lets the kernel close the connection |
| llama.cpp pause and cut never detected | never (600 s) | right |
| llama.cpp recovery manual | manual; after a cut the remote host also needs restarting unless it was mid-send (~950 s) | right, with a mechanism |

## 9. Not measured

vLLM partition; disaggregated prefill/decode; any system with in-request recovery
(Petals would be the one); C as a function of prompt length on exo and vLLM (measured on
llama.cpp only, §5); real network conditions (`tc netem`).
