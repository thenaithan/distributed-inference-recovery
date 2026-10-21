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
ConnectToGroup → LoadModel (weights from disk) → Warmup (`plan.py:57-68`) with a 400 s
initialise timeout (`supervisor.py:216`) and up to 5 attempts with 0.5 s to 10 s backoff
(`worker/main.py:114-117`, `constants.py:114`). KV lives in the runner process
(`engines/mlx/cache.py:244-256`) and dies with it.

**Master loss:** zenoh `Expired` or 30 s heartbeat silence → 3 s election → the new
master drops all generation tasks (`master/main.py:173-187`); the API ends every
in-flight stream with a "generation interrupted" message (`api/main.py:227-229,
346-356`). Instances survive a master change; requests do not.

**Client retry:** the API resends a `TextGeneration` command that was never accepted at
10, 15 and 30 s (`api/main.py:236`); an accepted-then-failed generation is not retried.

**Two things found while running it (NOTES-2026-10-21.md):** the API sends an SSE
`: keep-alive` every 10 s while nothing happens (`api/keepalive.py`), so a client cannot
detect a failure by socket timeout; detection is token-based and the client's effective
detection time is whatever the server's own detector takes to emit the error chunk.
And the 90 s stall detector (`supervisor.py` `_stuck()`) runs only on rank 0 with a
generation in progress, and its timer is reset by **any** runner event, not only tokens;
whether a paused peer leaves it un-reset is a prediction the pause trials test.

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
