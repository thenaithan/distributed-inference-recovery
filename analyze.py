#!/usr/bin/env python3
"""Turn trial logs from exo/ or vllm/ into C and its parts, in seconds and in tokens,
and into churnsim parameters.

    python3 analyze.py exo/trials.jsonl
    python3 analyze.py vllm/trials.jsonl
    python3 analyze.py exo/trials.jsonl --regime tvbox-26b     # restate C in the article's regime

Per trial, seconds after the injected failure (t_fail) unless marked:

  detect        the client learns the request is dead (t_stream_end). Token-based: exo's
                SSE keep-alives make socket timeouts useless, so this is whatever the
                server's own detector took to emit the error chunk.
  gone          the system has acted (exo: InstanceDeleted; vllm: server exited)
  replace_wait  exo only: placement refused (400) until the dead node is dropped
                (t_replace_cmd - t_replace_try)
  load          weights loaded on the replacement (from the runner status timeline:
                RunnerLoading -> RunnerLoaded), or the whole place->ready span if the
                timeline is missing
  warmup        RunnerWarmingUp -> RunnerReady; exo decodes WARMUP_TOKENS tokens here
  prefill       resubmitted request -> first token; prompt_tokens from the usage field
  C             first token of the resubmitted request: the full cost of one failure for
                a request that is then restarted from scratch

Token units: warmup = warmup_tokens decode tokens, prefill = prompt_tokens prefill tokens.
With --regime, the warmup and prefill phases of C are replaced by warmup_tokens and
prompt_tokens at per-token speeds DERIVED from the Frontier Red Team article's figures
(STUDY.md §"The self-replicating AI article"); everything else in C (detect, waiting for
the instance to be deleted, placement refusal, runner creation, load) is kept as measured. This is a restatement under a stated
assumption, not a measurement.

Mapping to churnsim: both systems are Strategy.RESTART. detect_time = detect;
reconnect_time = C minus prefill (the sim charges the re-run of the prefix as hops).
"""

from __future__ import annotations

import argparse
import json
import statistics as st

# per-token seconds, (low, high), derived from the article's own figures: see STUDY.md
REGIMES = {
    "tvbox-26b": dict(decode=(15.0, 20.0), prefill=(1.0, 1.35),
                      note="Gemma4-26B-A4B on 15 Android TV boxes, 32k window, derived"),
    "tvbox-9b": dict(decode=(6.0, 7.0), prefill=(0.39, 0.46),
                     note="Qwen3.5-9B on the same swarm, same token mix assumed, derived"),
}


def pick(rec, *keys):
    for k in keys:
        if rec.get(k) is not None:
            return rec[k]
    return None


def detected(rec):
    """The system ended the stream after the failure. "harness_timeout" means the trial
    harness gave up first: not detected within its stall timeout, not a detection time."""
    return bool(rec.get("t_fail")) and rec.get("end_kind") != "harness_timeout"


# exo's runner states in order; a phase ends when every runner is at or past its end state,
# so a state the 0.25 s poll never sampled (RunnerLoaded lasts ~1 s) does not lose the phase.
RUNNER_ORDER = ["RunnerIdle", "RunnerConnecting", "RunnerConnected", "RunnerLoading",
                "RunnerLoaded", "RunnerWarmingUp", "RunnerReady", "RunnerRunning"]


def at_or_past(status, end_status):
    return status in RUNNER_ORDER and RUNNER_ORDER.index(status) >= RUNNER_ORDER.index(end_status)


def phase(timeline, start_status, end_status):
    """Seconds between the first time any runner shows start_status and the first time
    all runners are at or past end_status, from a [[t, {rid: status}], ...] timeline.
    None if absent."""
    if not timeline:
        return None
    t_start = next((t for t, s in timeline if start_status in s.values()), None)
    t_end = next((t for t, s in timeline if s and all(at_or_past(v, end_status) for v in s.values())), None)
    if t_start is None or t_end is None or t_end < t_start:
        return None
    return t_end - t_start


def parts(r):
    """Decompose one trial. Values are None when the record lacks them."""
    f = r.get("t_fail")
    tl = r.get("replace_status_timeline") or []
    out = dict(
        detect=(r["t_stream_end"] - f) if detected(r) else None,
        gone=(pick(r, "t_instance_gone", "t_server_exit") - f) if f and pick(r, "t_instance_gone", "t_server_exit") else None,
        replace_wait=(r["t_replace_cmd"] - r["t_replace_try"]) if r.get("t_replace_cmd") and r.get("t_replace_try") else None,
        load=phase(tl, "RunnerLoading", "RunnerLoaded"),
        warmup=phase(tl, "RunnerWarmingUp", "RunnerReady"),
        place_to_ready=(r["t_replace_ready"] - r["t_replace_cmd"]) if r.get("t_replace_ready") and r.get("t_replace_cmd") else
                       ((r["t_ready_again"] - r["t_relaunch"]) if r.get("t_ready_again") and r.get("t_relaunch") else None),
        prefill=(r["t_resubmit_first_token"] - r["t_resubmit_request"]) if r.get("t_resubmit_first_token") and r.get("t_resubmit_request") else None,
        C=(r["t_resubmit_first_token"] - f) if f and r.get("t_resubmit_first_token") else None,
        prompt_tokens=(r.get("resubmit_usage") or r.get("usage") or {}).get("prompt_tokens"),
        warmup_tokens=r.get("warmup_tokens"),
    )
    # steady decode: before the injection (skip the last interval), or all of a failure-free run
    cut = None if r.get("mode") == "none" else max((r.get("at_token") or 10**9) - 2, 0)
    its = r.get("inter_token_s", [])[:cut]
    out["decode_s_per_tok"] = st.median(its) if its else None
    return out


def fmt(vals, unit="s"):
    vals = [v for v in vals if v is not None]
    if not vals:
        return "      -"
    return f"{st.median(vals):7.1f} {unit} ({min(vals):.1f}–{max(vals):.1f}, n={len(vals)})"


def summarize(rows, label):
    P = [parts(r) for r in rows]
    print(f"\n{label}   median (min–max, n)")
    for key in ("detect", "gone", "replace_wait", "load", "warmup", "place_to_ready", "prefill", "C"):
        print(f"  {key:<14}", fmt([p[key] for p in P]))
    undetected = [r for r in rows if r.get("t_fail") and not detected(r)]
    if undetected:
        print(f"  not detected within the harness stall timeout: {len(undetected)}")
    toks = [p["prompt_tokens"] for p in P if p["prompt_tokens"]]
    dec = [p["decode_s_per_tok"] for p in P if p["decode_s_per_tok"]]
    if toks:
        wt = next((p["warmup_tokens"] for p in P if p["warmup_tokens"]), None)
        print(f"  prefill tokens: {st.median(toks):.0f};  warmup tokens: {wt}")
    if dec:
        print(f"  steady decode: {st.median(dec):.2f} s/token (median of per-trial medians)")
    kinds: dict = {}
    for r in rows:
        kinds[r.get("end_kind")] = kinds.get(r.get("end_kind"), 0) + 1
    print("  stream ended by:", kinds)
    return P


def restate(P, regime):
    """C in another regime: measured timers + token counts × that regime's per-token speeds."""
    R = REGIMES[regime]
    print(f"\nrestated in regime '{regime}' ({R['note']}):")
    rows = []
    for p in P:
        if None in (p["C"], p["warmup"], p["prefill"]) or not p["prompt_tokens"] or not p["warmup_tokens"]:
            continue
        # Everything in C except warmup and prefill stays as measured: detection, waiting
        # for the instance to be deleted, placement refusal, runner creation and load.
        fixed = p["C"] - p["warmup"] - p["prefill"]
        lo = fixed + p["warmup_tokens"] * R["decode"][0] + p["prompt_tokens"] * R["prefill"][0]
        hi = fixed + p["warmup_tokens"] * R["decode"][1] + p["prompt_tokens"] * R["prefill"][1]
        rows.append((lo, hi))
    if not rows:
        print("  (no complete trials)")
        return
    lo = st.median([a for a, _ in rows]); hi = st.median([b for _, b in rows])
    print(f"  C ≈ {lo/60:.0f}–{hi/60:.0f} min for this prompt; "
          f"a 28k-token prefill alone would be {28000*R['prefill'][0]/3600:.1f}–{28000*R['prefill'][1]/3600:.1f} h")
    print("  (C as measured with its warmup and prefill phases replaced by the regime's per-token "
          "speeds; the prefill of exo's short warmup prompt is left out, so slightly low; "
          "assumption, not measurement)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--regime", choices=sorted(REGIMES))
    args = ap.parse_args()
    rows = [json.loads(l) for l in open(args.path) if l.strip()]
    by_mode: dict = {}
    for r in rows:
        # master loss takes a different path (election) from worker loss: keep them apart
        key = r.get("mode") + ("-master" if r.get("master_node") is not None and r.get("master_node") == r.get("fail_node") else "")
        by_mode.setdefault(key, []).append(r)
    allP = {}
    for mode, rs in by_mode.items():
        allP[mode] = summarize(rs, f"{args.path}  mode={mode}")
    print("\nchurnsim parameters (medians over detected trials):")
    for mode in ("kill", "pause", "cut"):
        P = allP.get(mode, [])
        d = [p["detect"] for p in P if p["detect"] is not None]
        if d:
            print(f"  detect_time[{mode}] = {st.median(d):.1f}")
    kills = allP.get("kill", [])
    rc = [p["C"] - (p["prefill"] or 0) for p in kills if p["C"] is not None]
    if rc:
        print(f"  reconnect_time (restart; C minus prefill) = {st.median(rc):.1f}")
    if args.regime:
        for mode, P in allP.items():
            if mode != "none":
                restate(P, args.regime)


if __name__ == "__main__":
    main()
