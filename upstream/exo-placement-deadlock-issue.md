# Draft GitHub issue for exo-explore/exo

Status: not yet filed. Paste at https://github.com/exo-explore/exo/issues/new (bug
template) or `gh issue create --repo exo-explore/exo --title "<title>" --body-file
upstream/exo-placement-deadlock-issue.md` after deleting this header block. Record the
issue number here once filed.

---

**Title:** [BUG] Placement deadlocks ~1 in 15 times: runner stuck in `mx.distributed.init`, status never reaches the master, node wedged until restart

## Summary

About one placement in fifteen never becomes ready. The runners stay `RunnerIdle` in the
cluster state forever, nothing times out, and afterwards every placement that includes
the affected node hangs the same way. Only restarting that node's exo process clears it.

Seen 5 times in ~70 placements over three days, on fresh 3-node placements and on 2-node
re-placements after a node left.

## Environment

- exo `bc412e2` (2026-10-13), `uv sync --extra mlx-cpu`; the code paths below are
  unchanged on `main` as of 2026-10-23
- Linux x86_64, Python 3.13, 3 nodes in rootless Podman containers on one host, MLX CPU
  backend (PyPI mlx + mlx-cpu 0.32.0, see #2359)
- `MlxRing`, `Pipeline`, `mlx-community/Qwen3-0.6B-8bit`

## Reproduce

`POST /place_instance` with `{"model_id": "mlx-community/Qwen3-0.6B-8bit", "sharding":
"Pipeline", "instance_meta": "MlxRing", "min_nodes": 3}`, poll `GET /state`. It is a
race; repeating the placement on a small model hits it within a few dozen attempts.

## Symptom

Runner statuses stay `RunnerIdle` indefinitely (we waited up to an hour). The stuck
node's log ends with:

```
[ 01:37:25.6523AM | INFO ] runner created
[ 01:37:25.6833AM | INFO ] runner connecting
[ 01:37:25.6835AM | INFO ] Starting initialization for rank 1
```

with no `initialization complete`. The node holding the last rank logs only `runner
created`: it is never sent `ConnectToGroup`. After `DELETE /instance/<id>` the stuck
node does not act on the `Shutdown` either, and later instances that include it never
form.

## Cause, as far as the source shows

The last rank is told to connect only when the state shows every other runner as
`RunnerConnecting` (`worker/plan.py:199-211`). A runner sets `RunnerConnecting`,
acknowledges the task, then blocks in `mx.distributed.init` (`runner/runner.py:237-242`,
`engines/mlx/utils_mlx.py:121`). Both messages go through a `multiprocessing.Queue`
(`utils/channels.py:231`), which hands them to a feeder thread that needs the GIL, and
MLX's `init` binding holds the GIL for its whole duration (mlx `python/src/
distributed.cpp`, no `gil_scoped_release`, on v0.32.0 and main). If `init` gets the GIL
before the feeder thread has written the two messages, they never leave the process:
the master never sees `RunnerConnecting`, the last rank never connects, `init` never
returns.

Nothing bounds this: `initialize_timeout` (400 s) is stored but never read
(`runner/supervisor.py:193,216,244`), and the stall detector needs a generation in
progress. It is sticky because the worker awaits the acknowledgement in `start_task`
(`supervisor.py:315`), so the node's planning loop stops for good.

This fits the code and all five occurrences; we have not yet confirmed it with a thread
dump and will attach one at the next occurrence.

## Suggested fix

Turn a runner that does not acknowledge or connect within a bound into `RunnerFailed`
(the unused `initialize_timeout` fits), and do not block the node's planning loop on one
runner's acknowledgement. Flushing the status/ack sends synchronously before `connect`,
or releasing the GIL around `distributed::init` in MLX, would remove the race itself.

## Workaround

Restart exo on the node whose log has `Starting initialization` without `initialization
complete`, then re-place.
