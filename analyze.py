#!/usr/bin/env python3
"""Turn trial logs from measure/exo or measure/vllm into C and its parts, and into
churnsim parameters.

    python3 measure/analyze.py measure/exo/trials.jsonl
    python3 measure/analyze.py measure/vllm/trials.jsonl

Per trial, all in seconds after the injected failure (t_fail):

  detect      when the client learns the request is dead (t_stream_end)
  gone        when the system has acted on the loss (exo: instance deleted;
              vllm: server exited)
  recover     when a replacement is serving again (exo: re-placed instance ready;
              vllm: relaunched server healthy)
  C           first token of the resubmitted request: the full cost of one failure
              for a request that is then restarted from scratch

Also reports the steady-state inter-token time (hop time S*h for churnsim).

Mapping to churnsim: both systems are Strategy.RESTART with reconnect_time = C minus the
re-run of the prefix (which the sim charges separately as hops). detect_time = detect.
"""

from __future__ import annotations

import json
import statistics as st
import sys


def pick(rec, *keys):
    for k in keys:
        if rec.get(k) is not None:
            return rec[k]
    return None


def detected(rec):
    """The system ended the stream after the failure. "harness_timeout" means the trial
    harness gave up first: not detected within its stall timeout, not a detection time."""
    return bool(rec.get("t_fail")) and rec.get("end_kind") != "harness_timeout"


def summarize(rows, label):
    def col(f):
        vals = [f(r) for r in rows]
        vals = [v for v in vals if v is not None]
        if not vals:
            return "   -   "
        return f"{st.median(vals):6.1f} ({min(vals):.1f}–{max(vals):.1f}, n={len(vals)})"
    print(f"\n{label}  [median (min–max, n)] seconds after failure")
    print("  detect  ", col(lambda r: r["t_stream_end"] - r["t_fail"] if detected(r) else None))
    undetected = [r for r in rows if r.get("t_fail") and not detected(r)]
    if undetected:
        print(f"  not detected within the harness stall timeout: {len(undetected)} "
              f"(stall_timeout {sorted({r.get('stall_timeout') for r in undetected}, key=str)})")
    print("  gone    ", col(lambda r: (pick(r, "t_instance_gone", "t_server_exit") - r["t_fail"])
                            if r.get("t_fail") and pick(r, "t_instance_gone", "t_server_exit") else None))
    print("  recover ", col(lambda r: (pick(r, "t_replace_ready", "t_ready_again") - r["t_fail"])
                            if r.get("t_fail") and pick(r, "t_replace_ready", "t_ready_again") else None))
    print("  C       ", col(lambda r: (r["t_resubmit_first_token"] - r["t_fail"])
                            if r.get("t_fail") and r.get("t_resubmit_first_token") else None))
    its = [x for r in rows for x in r.get("inter_token_s", [])[:r.get("at_token", 10**9) - 2]]
    if its:
        print(f"  steady inter-token time: median {st.median(its)*1000:.0f} ms, "
              f"p90 {sorted(its)[int(0.9*len(its))-1]*1000:.0f} ms (n={len(its)})")
    kinds = {}
    for r in rows:
        kinds[r.get("end_kind")] = kinds.get(r.get("end_kind"), 0) + 1
    print("  stream ended by:", kinds)


def main(path):
    rows = [json.loads(l) for l in open(path) if l.strip()]
    by_mode = {}
    for r in rows:
        by_mode.setdefault(r.get("mode"), []).append(r)
    for mode, rs in by_mode.items():
        summarize(rs, f"{path}  mode={mode}")
    # churnsim handoff
    print("\nchurnsim parameters (median over kill trials):")
    kills = by_mode.get("kill", [])
    if kills:
        detect = st.median([r["t_stream_end"] - r["t_fail"] for r in kills if detected(r)] or [float("nan")])
        C = [r["t_resubmit_first_token"] - r["t_fail"] for r in kills if r.get("t_fail") and r.get("t_resubmit_first_token")]
        print(f"  detect_time = {detect:.1f}")
        if C:
            print(f"  reconnect_time (restart, incl. relaunch) = {st.median(C):.1f}")
    pauses = by_mode.get("pause", [])
    if pauses:
        detect = st.median([r["t_stream_end"] - r["t_fail"] for r in pauses if detected(r)] or [float("nan")])
        print(f"  detect_time (silent hang) = {detect:.1f}")


if __name__ == "__main__":
    main(sys.argv[1])
