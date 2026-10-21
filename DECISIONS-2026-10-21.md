# Decisions and task list, 2026-10-21

Response to NOTES-2026-10-21.md (your session notes). Decisions first, then what is
already done on the `option1` branch, then the tasks that need the trial machine.

## Numbering

This is **Experiment 3**: the third experiment carried out (1 churn simulation,
2 cuts, 3 measured recovery cost). Traffic fingerprinting, which was "3" in the original
plan, becomes Experiment 4. All documents in this repo now say Experiment 3; the public
repo has no numbered forward references, so nothing changes there.

## Decisions on the open points (NOTES §6)

1. **Option 1: stay on CPU, design around the speed.** The quantities that are
   hardware-independent (detection, instance deletion, placement refusal, orchestration
   path) are measured as seconds. The hardware-dependent ones (load, warmup, prefill)
   are measured in seconds *and* tokens and restated in the article's regime with its
   derived per-token speeds (`analyze.py --regime`). The 15× prefill/decode mismatch
   becomes a stated conversion rather than a distortion. Option 3 (rent GPUs) stays in
   reserve only if a reader asks for C as a curve in context length; its shape is known
   (linear in tokens plus an attention term) and what we lack is the target hardware's
   constants, which the article supplies. Option 2 is dropped.
2. **Context length: not measured.** Report prefill per token and extrapolate; the
   article's 28k-token prefill becomes a stated figure with a stated assumption.
3. **The article goes into STUDY.md** as the framing section (added), with: the Petals
   dependence (our model is fitted to the 2023 NeurIPS paper, arXiv:2312.08361, where
   Table 1 lives; the article cites the 2022 demo paper, arXiv:2209.01188, same
   Algorithm 1, no fault-tolerance measurement, so "same mechanism, measured in the later
   paper"), the uncosted-replacement observation, the derived-hours hypothesis, and the
   R₀ connection. Figures derived from the article stay marked **derived**.
4. **Pinning: off.** One methods line (nodes share 20 logical CPUs, OS-scheduled across
   P- and E-cores). Pinning changes speed, not timeouts.
5. **Network emulation: stated limitation.** Our virtual network is near-ideal; the
   constants we measure are timers. `tc netem` matters only for detection under loss,
   which is a different question.
6. **Report the mlx-cpu pairing bug upstream** with the `nm` table from NOTES §2. It is
   a clean finding and it is the kind of methods detail that gets a study trusted.
7. **Scope line, now in STUDY.md:** measure and model; do not build the automatic
   re-placement exo lacks. That is the piece a replicator needs and it is not ours to supply.
8. **4.4 (vLLM on one GPU): agreed.** Both systems on CPU, which also keeps the vLLM
   path we measure the one FINDINGS-source.md analysed.
9. **4.1 is a result in its own right:** a client cannot detect failure by socket
   timeout because exo keeps the SSE alive every 10 s, so detection is token-based and
   the client's effective detection time is whatever the server's own detector takes.
   Added to FINDINGS-source.md.
10. **4.2 stays a prediction** until the pause trials settle it; FINDINGS-source.md keeps
    90 s with a note that the detector is reset by any runner event.

## Done on branch `option1` (not run; check with `--mode none` first)

- `exo/trial.py`: defaults `--at-token 5`, `--max-tokens 30`; records a runner status
  timeline during every placement (`place_status_timeline`, `replace_status_timeline`)
  so load (RunnerLoading→RunnerLoaded) and warmup (RunnerWarmingUp→RunnerReady) separate;
  requests `stream_options.include_usage` and records `usage` / `resubmit_usage`
  (prompt_tokens = prefill size); records `t_resubmit_request`; `warmup_tokens = 50`
  recorded per trial (exo's warmup decode length, from the runner logs; verify once).
- `analyze.py`: decomposition detect / gone / replace_wait / load / warmup /
  place_to_ready / prefill / C, token counts, steady decode speed, churnsim parameters,
  and `--regime tvbox-26b|tvbox-9b` restatement. Exercised on a synthetic record only.
- `STUDY.md`: article section; predictions updated for early injection; scope line;
  numbering.
- `FINDINGS-source.md`: keep-alive finding; stall-detector precondition note; numbering.
- `README.md`: quick start with the new defaults; numbering.

## Tasks for the trial machine

1. `git fetch && git checkout option1`. Rebuild nothing; the image is unchanged.
2. `python3 exo/trial.py --nodes 3 --mode none --out exo/smoke.jsonl` and check that the
   record has `usage.prompt_tokens`, a non-empty `place_status_timeline` with
   RunnerLoading / RunnerLoaded / RunnerWarmingUp / RunnerReady in it, and ~30 tokens.
   If `usage` is empty, exo may not honour `stream_options`; then take prompt_tokens
   from the runner log once and hard-code it in the record (prompt is fixed).
3. Confirm `WARMUP_TOKENS = 50` against a runner log line from this build.
4. Trials, 10 each, in this order (each ~10 to 15 min on CPU):
   - `kill` on node 1 (worker): `exo/cluster.sh up 3` between trials to restart it.
   - `pause` on node 1: this is the one that tests NOTES §4.2. Note whether the stream
     ends from the master's InstanceDeleted (~15 s) or the runner stall (90 s or more).
   - `cut` on node 1.
   - `kill` on node 0 (master): expect "generation interrupted" after election; the
     harness's API base then has to move to a surviving node (`--base http://<ip>:52415`
     from `cluster.sh ip 1`); do 3 trials to start, it is the least rehearsed path.
5. `python3 analyze.py exo/trials.jsonl` and `--regime tvbox-26b`; paste the output
   here with the `trials.jsonl`.
6. vLLM: `python3 vllm/trial.py --mode none`, then 10 × `kill`, 10 × `pause`. The
   `pause` trials will each take ≥ 300 s by construction (`VLLM_EXECUTE_MODEL_TIMEOUT_SECONDS`).
7. Merge `option1` to `main` in this repo when the smoke test passes; nothing goes to
   the public repo until there are numbers and a results section.

## What I need back

`exo/trials.jsonl`, `vllm/trials.jsonl`, the `analyze.py` output, and for the pause
trials one note on which detector fired. I will then write RESULTS.md, update the
resilience model's constants, and draft the one-paragraph addition to the public
repo's applicability section for you to edit.
