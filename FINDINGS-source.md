# What exo and vLLM do when a node fails: read from source

Before measuring anything, both systems were read for their failure paths, the way the
Petals client was read for NOTES.md §4. Every claim below cites file:line in the
checked-out commit; constants are the defaults in code, not documentation.

- exo: `exo-explore/exo` at `bc412e2` (2026-10-13), Python under `src/exo`, networking in Rust.
- vLLM: `vllm-project/vllm` at `8407bc0` (2026-10-20), V1 engine.

The short version: **both are restart-from-scratch, and neither restarts anything by
itself.** Petals (2023) is the only one of the three with in-request recovery.

| | Petals (NOTES §4) | exo | vLLM, plain PP | vLLM, disaggregated P/D |
|---|---|---|---|---|
| What survives a worker failure | client-side input cache, replayed to a replacement | nothing | nothing | KV on the prefiller for a 30 s lease; else nothing |
| Detection of a crashed worker | next send fails (one hop) | 5 s runner poll on ring error; 15 s node-silence at master | immediate (process sentinel) | immediate on that instance; D's pull fails after 5 s handshake timeout |
| Detection of a silently hung worker | `request_timeout` 180 s | 90 s runner stall timeout | `VLLM_EXECUTE_MODEL_TIMEOUT_SECONDS` 300 s | no vLLM-side transfer timeout (UCX/NIXL error or nothing) |
| What happens to the request | re-routed, prefix replayed, continues | error chunk; client must resubmit | `EngineDeadError`, HTTP 500; client must resubmit | `FINISHED_ERROR` (default) or local re-prefill (`recompute`) |
| What happens to the instance | nothing | whole instance deleted after 15 s silence; **no automatic re-placement** | engine core and API server exit; **no automatic restart** | same, per instance |
| Cost to serve again | one DHT lookup + connect + replay | API call to re-place + full weight reload (400 s init timeout) | relaunch + weight reload + warm-up (600 s ready timeout) | same |
| Replication of a shard | several servers per block, DHT-advertised | none within an instance (bijection shard ↔ runner) | none | none |
| Strategy in churnsim terms | **replay** | **restart** | **restart** | **restart** (or re-prefill on D) |

## exo (bc412e2)

**Architecture.** Every process runs Router, Worker, Master, Election and API
(`src/exo/main.py:52-66, 138-148`); one node is master, the others demote (`:264-272`).
The master is an event-sourcer: commands in, events out, every node applies the same
events to an identical `State` (`master/main.py:675-707`). A model instance is one
placement over a cycle of nodes; each node runs one runner subprocess holding one
pipeline shard (`shared/types/worker/shards.py:73-74`). Activations move rank to rank
over an MLX ring (TCP) or RDMA group (`engines/mlx/auto_parallel.py:131-137`); only rank
0 emits tokens (`runner/supervisor.py:418-425`). "Disaggregated" here is optional
prefill/decode separation (`constants.py:110`), a one-shot KV push over a raw socket,
not a recovery mechanism.

**Discovery and liveness.** Not mDNS or libp2p any more: custom IPv6 multicast
Hello/WhatsUp on `ff12::e0a1:de89`, UDP 52413, announced every 1 s
(`rust/networking/src/discovery.rs:19, 138, 469-488`); zenoh pub/sub over TCP 52414.
Liveness is zenoh liveliness tokens with the zenoh default lease (10 s;
`discovery.rs:32`, not overridden in `lib.rs:262-291`). On top: workers report every
1 s (`info_gatherer.py:451-453`), the master drops a node after 15 s of silence
(`master/main.py:91, 629-643`); the master heartbeats every 5 s and followers call an
election after 30 s (`election.py:23, 27`); an election round is 3 s (`election.py:20`).

**Worker loss mid-generation** (`supervisor.py:391-407, 470-500`; `master/main.py:615-653`):
the surviving ranks' ring sends fail; the supervisor polls runner stderr every 5 s
(`RUNNER_WATCH_INTERVAL`, `supervisor.py:61`), kills the runner and emits an
`ErrorChunk` for every in-flight task. If no ring error surfaces, rank 0 is declared
stuck after 90 s without a token (`RUNNER_STALL_TIMEOUT`, `supervisor.py:64`). In
parallel the master's 1 s plan loop emits `NodeTimedOut` after 15 s of silence and
`InstanceDeleted` for every instance that node was part of; workers shut down the
orphaned runners (`plan.py:76-79`). **Nothing re-places the instance**: `PlaceInstance`
is only ever issued from API handlers (`api/main.py:484, 534, 599`). A rejoining node
gets a fresh random `NodeId` (`router.py:256`). A new placement runs CreateRunner →
ConnectToGroup → LoadModel (weights from disk) → Warmup (`plan.py:57-68`), up to 5
attempts with 0.5 s to 10 s backoff (`worker/main.py:114-117`, `constants.py:114`). The
supervisor is created with an `initialize_timeout` of 400 s (`supervisor.py:216`), but
nothing reads it: a placement whose ranks never connect has no timeout (see below). KV lives in the runner process
(`engines/mlx/cache.py:244-256`) and dies with it.

**Master loss:** zenoh `Expired` or 30 s heartbeat silence → 3 s election → the new
master drops all generation tasks (`master/main.py:173-187`). Measured over 20 master
losses (10 kills, 5 pauses, 5 partitions, 2026-10-22/23), detection takes one of three
paths, and which one is not deterministic:

- 3.3 s: the dying container closes its TCP links, a survivor logs `expired: <id>` within
  0.1 s (`swarm.rs:89`, liveliness token deleted), a `Connection` message starts the
  election 0.2 s later (`election.py:300-318`). Kill only: 3 of 10.
- 13.2 s: no close reaches the peers (frozen process or removed interface), so zenoh's
  link lease runs out at its default 10 s (`transport/link/tx/lease`, exo leaves it at the
  default, `rust/networking/src/lib.rs`), the token is deleted, and the election follows.
  All 5 partitions and 3 of 5 pauses; in each case exactly one survivor logged the expiry.
- 30–36 s: no survivor ever logs an expiry (checked to 400 s). The follower's 30 s
  MASTER_SILENCE_TIMEOUT fires, then the election. 7 of 10 kills, 2 of 5 pauses. exo's own
  comment predicts this: in a cluster of three or more a crashed node's token "can be
  seen indefinitely" (`election.py:24-27`), so the 30 s timeout is the backstop.

`gone` is then the new master's 15 s NODE_SILENCE clock on the dead node (18.3, 28.4 or
45–51 s after the failure). Which path a loss takes correlated with which container was
lost (node 1: slow in all 4; node 2: fast in 6 of 7) rather than with the mode, which
points at the zenoh link topology formed at start-up; not pinned down. The API ends every
in-flight stream with a "generation interrupted" message (`api/main.py:227-229,
346-356`). Instances survive a master change; requests do not.

**Client retry:** the API resends a `TextGeneration` command that was never accepted at
10, 15 and 30 s (`api/main.py:236`); an accepted-then-failed generation is not retried.

**Three things found while running it (NOTES-2026-10-21.md):** the API sends an SSE
`: keep-alive` every 10 s while nothing happens (`api/keepalive.py`), so a client cannot
detect a failure by socket timeout; detection is token-based and the client's effective
detection time is whatever the server's own detector takes to emit the error chunk.
And the 90 s stall detector (`supervisor.py` `_stuck()`) runs only on rank 0 with a
generation in progress, and its timer is reset by **any** runner event, not only tokens.
Measured 2026-10-22, 10 worker pauses: the master's 15 s silence clock ended the stream
every time (14.5–15.9 s after the SIGSTOP, via `InstanceDeleted` and the error chunk),
so the stall detector was never reached; it matters only for a hang the master cannot
see, i.e. a runner that stops while its node keeps reporting. Ten worker partitions on
2026-10-23 (`podman network disconnect`, sockets left open) went the same way: detect
14.4–16.0 s, C 273.0–281.2 s. For a lost worker, exo's kill, pause and cut are one case:
the master's silence clock decides, not the shape of the failure.

The third is a deadlock in placement, hit twice in the 26 placements of 2026-10-21's 13
kill trials (once placing fresh on 3 nodes, once re-placing on the 2 survivors), and each
time every later placement wedged too until the cluster was recreated. The ranks of an
instance connect in order: every rank but the last calls `mx.distributed.init` as soon as
the runners exist, and the last rank is told to connect only once the state shows every
other runner as `RunnerConnecting` (`worker/plan.py:200-209`). A runner reports
`RunnerConnecting`, acknowledges the task, then blocks in `init` until all peers are in
(`runner/runner.py:237-247`, `engines/mlx/utils_mlx.py:121`). Both messages leave the
runner through a `multiprocessing.Queue` (`utils/channels.py:231`), whose `put` is
completed by a feeder thread that needs the GIL, and MLX's `init` binding holds the GIL
(mlx v0.32.0 `python/src/distributed.cpp:77`, no `gil_scoped_release`). When `init` gets
the GIL first, both messages stay inside the process: the state keeps the runner at
`RunnerIdle`, the last rank is never told to connect, and `init` never returns. Nothing
times out: `initialize_timeout` is unused and the 90 s stall detector needs a generation
in progress. The node's worker is meanwhile blocked in `start_task` waiting for the
acknowledgement (`supervisor.py:286-316`), so it stops acting on the cluster state at
all, including the `Shutdown` sent when the instance is deleted; later placements that
include the node wedge the same way, and only restarting its process clears it. Log
signature: the stuck node logs `runner connecting` and `Starting initialization for rank
N` but never `initialization complete`; the last rank's node logs `runner created` and
nothing more. The harness fails such a placement after 90 s (`CONNECT_TIMEOUT`). The GIL
explanation fits every observation; a thread dump of a stuck runner would make it certain.
Reported upstream as exo-explore/exo#2464 (2026-10-23); the mlx-cpu pairing bug the image
works around was already exo-explore/exo#2359, where we added the cause and the fix.

**Measurement hooks:** events `NodeTimedOut`, `InstanceDeleted`, `RunnerStatusUpdated`,
`TaskFailed`, `ChunkGenerated`/`ErrorChunk` (`shared/types/events.py:36-109`), stamped by
the master (`master/main.py:693-695`), readable at `GET /events` (last 10 000) and
`GET /state` (`api/main.py:443-446`). Tokens stream as SSE from `POST
/v1/chat/completions` (`api/main.py:408`).

**Platform:** MLX only; Linux CPU via `uv sync --extra mlx-cpu` (`pyproject.toml:62`,
README). Nodes need distinct IPs on one L2 with IPv6 multicast and identical ports, so
on one machine that means containers on an IPv6-enabled network (`measure/exo/`).
Smallest registered model: `mlx-community/Qwen3-0.6B-8bit`
(`resources/inference_model_cards/`).

## vLLM (8407bc0)

**Plain pipeline parallel, a worker process dies.** Detection is immediate and
OS-level: the multiprocessing executor blocks on the workers' process sentinels
(`vllm/v1/executor/multiproc_executor.py:306-325`); the Ray executor polls
`ray.wait(..., timeout=5.0)` (`ray_executor_v2.py:503-531`). On death the executor
shuts down all workers (`:459-507`), enqueues `EXECUTOR_FAILED` to the engine core
(`vllm/v1/engine/core.py:1157-1159`), which raises, sends `ENGINE_CORE_DEAD` to the
client and exits (`core.py:1550-1556, 1835-1845`). The client raises `EngineDeadError`
into **every** in-flight request (`async_llm.py:937-944`, `output_processor.py:593-597`);
HTTP clients get 500 (`entrypoints/serve/exception_handling/error_response.py:68-72`).
The API server's watchdog then exits the whole process after
`VLLM_WATCHDOG_TIME_S = 5` unless `VLLM_KEEP_ALIVE_ON_ENGINE_DEATH=1` (`envs.py:1097-1098`;
`launchers/launcher.py:314-337`). Ray actors have no `max_restarts` (`ray_executor_v2.py:383-390`).
**No automatic worker or engine restart**; the docs defer that to Kubernetes or Ray Serve
(`docs/serving/parallelism_scaling.md:69`; k8s liveness probe example: initial delay
60 s, period 10 s, `docs/deployment/k8s.md:244-255`).

**A worker that hangs instead of dying** is only noticed when a model RPC exceeds
`VLLM_EXECUTE_MODEL_TIMEOUT_SECONDS = 300` (`envs.py:249, 1845-1849`;
`multiproc_executor.py:349, 362, 436-442`). There is no worker heartbeat; `/health` only
reports whether the engine is already known dead (`async_llm.py:1129-1134`).

**Measured (vLLM 0.32.0 CPU wheel, PP=2, Qwen3-0.6B, 2026-10-24/25; 10 kills, 10 pauses, the
failed rank is PP1, injected at token 5).** Kill: the client sees the error after 1.5 s
(0.0–3.7). The sentinel does fire at once ("Worker proc VllmWorker-1 died unexpectedly"
is stamped in the same second as the kill), but the executor then waits for the surviving
rank to come out of its blocked gloo send to the dead peer before it reports upward; that
wait is 0 s when PP0 was between steps and up to 3.7 s when it was mid-send. The HTTP
port closes 4.5 s after the kill (4.0–8.6; the 5 s watchdog, counted from the error), the
relaunch takes 62 s (59–75; the warm-up compile runs on every start even with the
torch-compile cache warm), and C is 66.6 s (64.1–84.6). Pause: the client sees the error
after 300.0 s in all ten trials (300.0–300.1), exactly `VLLM_EXECUTE_MODEL_TIMEOUT_SECONDS`,
followed by "RPC call to sample_tokens timed out"; the port closes about 5 s later and the
relaunch takes 68 s (63–96), so C is 373 s (369–403, n=6). In the other four pause trials
the API server **process** outlived its closed port by more than 400 s (the harness then
waited for it, so their recorded C of 775–785 s is a harness artefact, not a system
number). The log differs in one place: on the six clean runs the shutdown logs "force
killing sub process VLLM::Worker_PP0 / PP1"; on the four it does not. `kill_process_tree`
(`vllm/utils/system_utils.py:208-233`) enumerates the engine core's children with psutil
at the moment it force-kills it; if the core has already left on its SIGTERM, the workers,
one of them still stopped, are orphaned rather than killed, and the server process does
not exit. What it then blocks on is not yet confirmed (a py-spy dump of a lingering server
would settle it). For an orchestrator probing the port the two cases are the same; for one
that watches the process, the hang case can also hang the restart.

**`--enable-fault-tolerance`** (new) is for data-parallel expert-parallel only: it aborts
all requests, marks the engine unhealthy and waits up to
`engine_recovery_timeout_sec = 120` for an orchestrator to POST a `retry`, which
re-initialises the DP group; it cannot recover a dead process
(`vllm/v1/fault_tolerance/engine_core_sentinel.py:87-133`, `config/fault_tolerance.py:12`).

**Disaggregated prefill/decode (NixlConnector).** The decoder's handshake to a dead
prefiller times out after 5 s (`kv_connector/v1/nixl/base_worker.py:1160-1161`). A
transfer that is in flight when a node vanishes has **no vLLM-side timeout**; detection
depends on UCX/NIXL surfacing an error (`base_worker.py:3405-3452`). The prefiller holds
KV blocks under a 30 s lease, extended by decoder heartbeats every 5 s
(`nixl/base_scheduler.py:82-88`, `base_worker.py:627-631, 3352-3399`). When a load
fails, `kv_load_failure_policy` decides: `fail` (default) finishes the request with
`FINISHED_ERROR`; `recompute` re-prefills locally on the decoder
(`config/kv_transfer.py:88-91`; `v1/core/sched/scheduler.py:3432-3445, 2396-2412`).
Mooncake: abort timeout 480 s, connector timeout 30 s (`envs.py:265-266`).

**Nothing preserves progress across a worker failure.** No KV checkpoint to another
worker, no replay log. The streamed tokens the client already has are the only state.

**Recovery cost** is a full relaunch: weights from disk, memory profiling, graph
capture, within `VLLM_ENGINE_READY_TIMEOUT_S = 600` (`envs.py:26, 809-811`). vLLM gives
no number; `docs/features/preload.md:15-17` says weight preloading cuts restart "from
minutes to seconds".

**Platform:** the CPU backend supports pipeline parallel across worker processes
(`platforms/cpu.py:333-340`; `docs/getting_started/installation/cpu.md:313`; CI runs
`-tp 2 -pp 2` on one CPU box). So `vllm serve <small model> -pp 2` on a laptop is a
valid measurement target (`measure/vllm/`).

## What this means for the model before any measurement

1. Of the three systems, the one with in-request recovery is the 2023 one. exo and
   vLLM are both the `restart` strategy of Experiment 1, which collapses once a
   generation expects about one failure, $p \approx 1/(ST)$. For them the relevant
   number is not λ* but the plain probability a request finishes: $(1-p)^{ST}$ per
   attempt, with $C$ = relaunch time added per failure.
2. Detection is bimodal in all three: fast for a clean crash (one hop, 5 s, immediate),
   slow for a silent hang (180 s, 90 s, 300 s). A node that stops without closing its
   sockets costs 20 to 60× more than one that crashes. The degrade cut of Experiment 2
   is the extreme case: a node that is slow but alive is never evicted by any of them.
3. "Self-healing" in Experiment 2 assumed joins re-fill an emptied stage. exo and vLLM
   do not re-fill anything on their own; the orchestrator (a human, Kubernetes) is the
   join process, and its rate is what `join_rate` stands for. For a swarm on machines
   nobody operates, that rate is zero and the static-swarm results apply.
4. The discovery layer in exo is an IPv6 multicast group on the local link plus a zenoh
   mesh; there is no DHT to cut. Partitioning the L2 or the multicast group is the
   registry cut. vLLM has no discovery at all within an instance; Ray's GCS or the
   Kubernetes control plane plays that role one level up.

## What the measurement should produce

For each system and failure mode (clean kill, silent hang, network cut), the
distribution of:

- detect: time from failure to the client learning its request is dead
- gone: time until the system has acted (instance deleted / server exited)
- recover: time until a replacement is serving (re-placed / relaunched and healthy)
- C: time from failure to the first token of the resubmitted request

plus steady-state inter-token time. `measure/analyze.py` computes these from the trial
logs and prints `detect_time` and `reconnect_time` for `churnsim`.

## llama.cpp RPC backend (74bca39, 2026-10-25)

`ggml-rpc-server` exposes a host's devices over TCP; the main host's `llama-server`
is started with `--rpc host:port,...` and `-ngl` to place layers on them, split by
`--tensor-split`. The layer slices form a pipeline across hosts, like the other two
systems. The README (`tools/rpc/README.md`) calls the backend a proof of concept,
"fragile and insecure", never to be run on an open network.

**No discovery.** The peer list is a command-line argument (`common/arg.cpp:1178-1192`,
`src/llama.cpp:191-276`). Nothing announces, nothing is looked up, nothing is
re-resolved. The "discovery layer" of the model is the operator's configuration.

**Failure handling: abort.** Every client-side RPC is wrapped in
`RPC_STATUS_ASSERT`, defined as `GGML_ABORT("Remote RPC server crashed or returned
malformed response")` (`ggml/src/ggml-rpc/ggml-rpc.cpp:31`); `send_msg`/`recv_msg`
return false on any transport error (`:323-346`) and the caller aborts the whole
`llama-server` process. Graph-compute failures are likewise asserted
(`:2192, 2211, 2349`: "Unsuccessful graph computations are not supported with RPC").
Connection setup aborts on failure (`:642-653`). The only retry in the file is a
100 × 50 ms loop for the server-to-server peer connection at startup (`:2258-2263`).

**No timeouts, no keepalive.** `transport.cpp` sets `TCP_NODELAY` (`:706`) and nothing
else on the socket: no `SO_RCVTIMEO`, no `SO_SNDTIMEO`, no `SO_KEEPALIVE`,
no `TCP_USER_TIMEOUT` (the one `timeout` in the file, `:391`, is an InfiniBand
queue-pair attribute). A blocking `recv` from a peer that has stopped answering
therefore never returns; a blocking `send` to one returns only when the kernel's
retransmission limit is reached (minutes, kernel default). The server side
(`tools/rpc/rpc-server.cpp`) has no timeouts either.

**Predictions for the harness** (`llamacpp/`):

| mode | what the main host sees | predicted detect | predicted recovery |
|---|---|---|---|
| `killproc` (SIGKILL the process; kernel sends RST) | `recv` fails → abort | one RPC, well under 1 s | relaunch `llama-server`: mmap load, seconds, plus prefill |
| `kill` (container torn down; packets dropped) | `recv` blocks | never, until the kernel's TCP retransmit limit on a pending send (≈15 min) or forever on a pure recv | manual |
| `pause` (SIGSTOP) | `recv` blocks | never | manual |
| `cut` (network disconnect) | `recv` blocks | never | manual |

So llama.cpp should show the sharpest version of the crash/hang asymmetry: sub-second
on a reset, unbounded on anything that does not reset. It is also the system where
prefill per token on CPU is a real number (batched, optimised), which the article-regime
restatement needs; `--prompt-tokens` in the harness sweeps it, and `llama-server`
reports `timings.prompt_ms` itself.

**Measured (2026-10-25; `ggml-org/Qwen3-0.6B-GGUF` Q8_0, two rpc hosts with one layer slice
each, 4 threads each, main host runs no layers; 61 actual prompt tokens; the failed host is
rpc-1).** killproc (n=8) and kill (n=5) are one case: the main host aborts 0.5 s after the
signal (0.5–0.8), its container is gone at 0.7 s, recreating the three containers and
reaching `/health` takes 13 s, prefill 0.2 s, C 13.9–14.2 s (13.8–14.9). The kill prediction
above was wrong for this injection: `podman kill` terminates the process before the
namespace goes, so the kernel still sends the close and the main host sees a reset. pause
(n=5): never detected; the harness gave up at 600 s in every trial, the main host still
blocked in `recv`; recovery is a main-host restart, 2 s, so C is 600 s of harness patience
plus about 3 s (the recorded 663 s includes a 60 s harness wait since removed). cut (n=5,
plus two earlier attempts that hit a 900 s ready timeout): never detected either, and the
heal does not help by itself. The rpc server serves one client at a time
(`ggml_backend_rpc_start_server`, accept → `rpc_serve_client` → accept) and stays inside
`rpc_serve_client` on the vanished main host's connection, so the restarted main host
connects but its hello is never answered. It gets free in exactly one way: if it was
mid-send when cut, the kernel exhausts retransmissions (`tcp_retries2` = 15, documented
lower bound 924.6 s) and the send fails; three trials came back at 950, 952 and 954 s after
the cut, the rpc host's own log dating the close to the second. If it was in `recv`, nothing
ever fires: two trials still hung at 1500 s and two more were recreated by the harness after
its 600 s restart window. A healed partition therefore heals llama.cpp RPC about half the
time, 15 minutes later; otherwise the remote host must be restarted. Prompt-length sweep
(killproc, n=2 each, prefill reported by `llama-server` on the fresh main host after the
relaunch, since the first stream of each trial hits the prompt cache of the previous one):
571 tokens 1.5 s, 2221 tokens 6.0 s, 6601 tokens 26.5 s, i.e. 2.6 to 4.0 ms per prompt
token against 26 ms per decoded token, a prefill/decode ratio of 7 to 10 on this CPU
(exo's MLX backend: about 1). C grows with it: 15.6, 19.9 and 40.4 s.

**Strategy:** restart, with the operator as the only recovery mechanism; no state of
the interrupted request survives (the KV cache for the remote layers lives in the
rpc-server's memory and is addressed by the dead process's buffer ids).
