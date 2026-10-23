# Draft comment for exo-explore/exo issue #2359

Status: not yet posted. #2359 ("[BUG] [Linux] mlx-cpu extra: ImportError undefined
symbol (ThreadLocalStream) on import mlx.core", open since 2026-09-30, no comments)
already reports this bug, so we add our evidence and fix there instead of opening a
duplicate. Post at https://github.com/exo-explore/exo/issues/2359 or with
`gh issue comment 2359 --repo exo-explore/exo --body-file upstream/exo-2359-mlx-cpu-comment.md`
after deleting this header block.

---

We hit the same failure at `bc412e2` (2026-10-13) on Linux x86_64, Python 3.13, and can
confirm the cause and a working fix.

**Cause.** `uv sync --extra mlx-cpu` installs the `mlx` binding from the
`rltakashige/mlx-jaccl-fix-small-recv` `mlx_cuda` release wheel (`[tool.uv.sources]`,
`pyproject.toml:85-86`) together with `mlx-cpu==0.31.2` from PyPI (`pyproject.toml:62`).
The binding's `core.cpython-313-x86_64-linux-gnu.so` is linked against the `libmlx.so`
of the fork at 0.32.0, and no PyPI `mlx-cpu` release matches it. `nm -D --undefined-only`
on the binding against the exported symbols of each `libmlx.so`:

| binding | `libmlx.so` from | unresolved `mlx::` symbols |
|---|---|---|
| fork wheel 0.32.0 (what the extra installs) | mlx-cpu 0.31.2 (what the extra pins) | 2 (`linalg::det`, `linalg::slogdet`) |
| fork wheel 0.32.0 | mlx-cpu 0.32.0 | 6 |
| fork wheel 0.32.0 | mlx-cpu 0.32.1 to 0.32.4 | ~300 |
| PyPI `mlx` 0.32.0 | mlx-cpu 0.32.0 | **0 of 402** |

That is why bumping `mlx-cpu` changes the missing symbol instead of fixing it: the
binding is from a different tree than any released CPU library. Which symbol you see
first depends on the pair (`linalg::det` for 0.31.2, `ThreadLocalStream`/`astype` for
others).

**Fix that works.** Use the official PyPI binding on Linux CPU, same version as
`mlx-cpu`, which is the pair upstream MLX declares for `mlx[cpu]`:

```sh
uv sync --extra mlx-cpu
uv pip install --python .venv/bin/python --no-deps --reinstall "mlx==0.32.0" "mlx-cpu==0.32.0"
.venv/bin/python -c "import mlx.core as mx; mx.eval(mx.linalg.det(mx.eye(2), stream=mx.cpu))"
```

Two traps: `--reinstall` is required because the fork wheel also calls itself 0.32.0, so
uv otherwise keeps it; and start exo with `uv run --no-sync exo`, because a plain `uv run`
re-syncs to `uv.lock` and reinstalls the fork wheel. With this, a 3-node `MlxRing`
pipeline instance on CPU places, loads and generates (we have run several hundred
requests this way).

**Suggested change in exo.** The fork only matters for JACCL (RDMA), which the CPU extra
cannot use. Either give the `mlx-cpu` extra its own source (PyPI `mlx==0.32.0`, and
`mlx-cpu==0.32.0` instead of 0.31.2), or publish a matching `libmlx.so` for the fork.
The `[tool.uv.sources]` markers would need to distinguish the extras, e.g.
`extra == 'mlx-cpu'` for the PyPI binding and `extra == 'mlx-cuda12' or extra ==
'mlx-cuda13'` for the fork wheel.
