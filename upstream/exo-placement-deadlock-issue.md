# Draft GitHub issue for exo-explore/exo

Status: not yet filed. File at https://github.com/exo-explore/exo/issues/new (bug
template) or with `gh issue create --repo exo-explore/exo --title "<title>" --body-file
upstream/exo-placement-deadlock-issue.md` after deleting this header block. Record the
issue number here once filed.

---

**Title:** [BUG] Placement can deadlock: runner's `RunnerConnecting` status never leaves the process, last rank is never told to connect, node stays wedged until its exo process is restarted

## Summary

About one placement in fifteen on our Linux cluster never becomes ready. The runners of
the new instance stay `RunnerIdle` in the cluster state forever: nothing times out, the
instance cannot be deleted cleanly, and every later placement that includes the affected
node wedges the same way. Only restarting that node's exo process clears it.

From the logs and the source the cause looks like an ordering race between the runner's
status/acknowledgement messages and `mx.distributed.init`, with no timeout anywhere on
the path. Details below. We are happy to supply full node logs or a thread dump of a
stuck runner.

## Environment

- exo `bc412e2` (2026-10-13), source install, `uv sync --extra mlx-cpu`; the code paths
  cited below are unchanged on `main` as of 2026-10-23
- Linux x86_64 host, Python 3.13, three exo nodes in rootless Podman containers on one
  host (one podman network, zenoh discovery), MLX backend on CPU
- mlx 0.32.0 and mlx-cpu 0.32.0 from PyPI (the fork wheel the `mlx-cpu` extra selects
  does not import, see #2359)
- instance: `MlxRing`, `Pipeline` sharding, `mlx-community/Qwen3-0.6B-8bit`, 2 or 3 nodes

## Steps to reproduce

1. Start 3 nodes; wait until all see each other.
2. `POST /place_instance` with `{"model_id": "mlx-community/Qwen3-0.6B-8bit",
   "sharding": "Pipeline", "instance_meta": "MlxRing", "min_nodes": 3}`.
3. Poll `GET /state` for the instance's runner statuses.

Usually all runners reach `RunnerReady` in a few seconds. We hit the hang 5 times in
roughly 70 placements over three days (2026-10-21 to 2026-10-23), both on fresh 3-node
placements and on 2-node placements after a node was removed. It is a race, so there is
no deterministic repro; placing repeatedly on a small CPU model gets it within a few
dozen attempts.

## What happens

Cluster state: the stuck runner(s) show `RunnerIdle` indefinitely (we waited 90 s to
400 s; one stayed for over an hour). Example, 2 nodes, 90 s after placement:

```
{'12508cea-…': 'RunnerIdle', 'd29d916e-…': 'RunnerIdle'}
```

Node logs. The stuck node logs the runner connecting and starting init but never
completing it:

```
[ 01:37:25.6523AM | INFO ] runner created
[ 01:37:25.6833AM | INFO ] runner connecting
[ 01:37:25.6835AM | INFO ] Starting initialization for rank 1
```

(normal runs continue with `initialization complete` and `runner connected` within a
second.) The node that holds the last rank logs `runner created` and nothing more: it is
never sent `ConnectToGroup`.

Afterwards:

- `DELETE /instance/<id>` removes the instance from the state, but the stuck node does not
  act on the resulting `Shutdown` (it logs nothing while the other nodes tear their
  runners down).
- Every later `place_instance` that includes the stuck node hangs the same way. Nodes
  not involved keep working.
- Restarting the stuck node's exo process clears it. Nothing short of that did.

## Why, as far as we can tell from the source

Ranks connect in order. Every rank but the last is told to `ConnectToGroup` as soon as
the runners exist; the last rank is told to connect only once the state shows every
other runner as `RunnerConnecting` (`src/exo/worker/plan.py:199-211`).

On `ConnectToGroup` a runner sets its status to `RunnerConnecting`, acknowledges the
task, and then calls `generator.connect`, which blocks in `mx.distributed.init(backend=
"ring", strict=True)` until all peers are in (`src/exo/worker/runner/runner.py:237-242`,
`src/exo/worker/engines/mlx/utils_mlx.py:121`).

Both the status update and the acknowledgement leave the runner process through a
`multiprocessing.Queue` (`src/exo/utils/channels.py:231`). `Queue.put` only appends to an
in-process buffer; a feeder thread pickles and writes it to the pipe, and that thread
needs the GIL. MLX's `init` binding is a plain nanobind `m.def` with no
`gil_scoped_release` (mlx v0.32.0 `python/src/distributed.cpp:76-81`; still a GIL-holding
lambda on mlx main), so once the main thread enters `init` the feeder thread cannot run
until `init` returns.

If the feeder thread gets to write both messages before the main thread enters `init`,
all is well. If `init` gets the GIL first, nothing leaves the process: the state keeps
the runner at `RunnerIdle`, the last rank is never told to connect, `init` waits for a
peer that will never come, and the feeder thread waits for a GIL that will never be
released. The log signature above (`Starting initialization` logged, status never seen
by the master) is exactly this.

Nothing times out:

- `RunnerSupervisor.initialize_timeout` (default 400 s) is stored but never read
  (`src/exo/worker/runner/supervisor.py:193,216,244`).
- The 90 s stall detector only runs for rank 0 with a generation in progress.

And it is sticky because the worker awaits the task acknowledgement in `start_task`
(`src/exo/worker/runner/supervisor.py:315`, `event.wait()` for every non-generation
task). That wait never ends, so the worker's planning loop for that node stops: it
ignores the later `Shutdown` and any new instance that includes the node can never form.

The GIL explanation fits every observation and the code, but we have not yet confirmed
it with a thread dump of a stuck runner. We will attach one at the next occurrence.

## Possible fixes

Any one of these would break the deadlock; the first two also cover other startup
hangs:

1. Bound the wait in `start_task` (or run a watchdog on `RunnerConnecting`) and turn a
   runner that does not acknowledge or connect in time into `RunnerFailed`, so the
   instance is torn down and the node keeps planning. `initialize_timeout` already exists
   for this and is unused.
2. Do not block the node's planning loop on a single runner's acknowledgement.
3. Make the status/acknowledgement sends synchronous (write to the pipe in the calling
   thread instead of via the `multiprocessing.Queue` feeder thread), or otherwise ensure
   they are flushed before `connect` is called.
4. Upstream in MLX: release the GIL around `distributed::init`
   (`nb::call_guard<nb::gil_scoped_release>()`), which would also let the feeder thread
   run. We can open an issue there if you agree with the diagnosis.

## Workaround

Restart the exo process on the node whose log shows `Starting initialization for rank N`
with no `initialization complete`, then delete and re-place the instance.
