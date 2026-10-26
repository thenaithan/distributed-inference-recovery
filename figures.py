"""Figures for the README, from the committed trial records.

    python3 figures.py            # writes figures/*.png

Every number plotted comes from analyze.parts() applied to exo/trials.jsonl,
vllm/trials.jsonl and llamacpp/trials.jsonl, plus the closed forms of the resilience
model for the last figure.
"""

from __future__ import annotations

import json
import math
import os
import statistics as st

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

import analyze as A  # noqa: E402

OUT = "figures"
os.makedirs(OUT, exist_ok=True)

# palette: one hue per system, fixed order; phases get their own fixed slots
SYS = {"exo": "#2a78d6", "vLLM": "#eb6834", "llama.cpp": "#1baf7a"}
PHASE = {"detect": "#2a78d6", "wait / relaunch": "#eb6834", "load": "#1baf7a",
         "warmup": "#eda100", "prefill": "#e87ba4"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
NEVER = 1e4   # where "never detected" is drawn on a log axis

plt.rcParams.update({
    "font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK,
    "axes.spines.top": False, "axes.spines.right": False,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.dpi": 160,
})


def load(path):
    rows = [json.loads(l) for l in open(path) if l.strip()]
    # same validity rule as analyze.py: a llama.cpp kill must have taken
    rows = [r for r in rows if not (r.get("system") == "llamacpp-rpc" and r.get("mode") in ("kill", "killproc")
                                    and r.get("t_fail") and r.get("rpc_state_after") != "exited")]
    return [r for r in rows if r.get("mode") != "none" and r.get("t_fail")]


exo = load("exo/trials.jsonl")
vllm = load("vllm/trials.jsonl")
lcpp = load("llamacpp/trials.jsonl")


def is_master(r):
    return r.get("master_node") is not None and r.get("master_node") == r.get("fail_node")


def c_valid(r, p):
    """C values that are recovery times: drop the vLLM shutdown artefact (RESULTS §4)."""
    if p["C"] is None:
        return None
    if r in vllm and r["mode"] == "pause" and p["C"] > 700:
        return None
    if r in lcpp and r["mode"] == "cut" and r.get("main_restart_stuck"):
        return None   # recreated by the harness, not a recovery
    return p["C"]


# rows of the strip plots: (label, system, records)
CASES = [
    ("exo, worker killed",        "exo", [r for r in exo if r["mode"] == "kill" and not is_master(r)]),
    ("exo, worker paused (hang)", "exo", [r for r in exo if r["mode"] == "pause" and not is_master(r)]),
    ("exo, worker partitioned",   "exo", [r for r in exo if r["mode"] == "cut" and not is_master(r)]),
    ("exo, master killed",        "exo", [r for r in exo if r["mode"] == "kill" and is_master(r)]),
    ("exo, master paused",        "exo", [r for r in exo if r["mode"] == "pause" and is_master(r)]),
    ("exo, master partitioned",   "exo", [r for r in exo if r["mode"] == "cut" and is_master(r)]),
    ("vLLM, PP worker killed",    "vLLM", [r for r in vllm if r["mode"] == "kill"]),
    ("vLLM, PP worker paused",    "vLLM", [r for r in vllm if r["mode"] == "pause"]),
    ("llama.cpp, rpc host killed",      "llama.cpp", [r for r in lcpp if r["mode"] in ("kill", "killproc") and r.get("prompt_tokens_requested") == 33]),
    ("llama.cpp, rpc host paused",      "llama.cpp", [r for r in lcpp if r["mode"] == "pause"]),
    ("llama.cpp, rpc host partitioned", "llama.cpp", [r for r in lcpp if r["mode"] == "cut"]),
]


def strip(ax, values_by_row, never_by_row, title, xlabel):
    n = len(CASES)
    for i, (label, sysname, _) in enumerate(CASES):
        y = n - 1 - i
        vals = values_by_row[i]
        ax.axhline(y, color=GRID, lw=0.6, zorder=0)
        if vals:
            ax.scatter(vals, [y] * len(vals), s=28, color=SYS[sysname], alpha=0.55,
                       edgecolor="white", linewidth=0.6, zorder=3)
            med = st.median(vals)
            ax.plot([med, med], [y - 0.32, y + 0.32], color=INK, lw=1.6, zorder=4)
            ax.text(med, y + 0.38, f"{med:.3g} s", ha="center", va="bottom", fontsize=8, color=INK)
        k = never_by_row[i]
        if k:
            ax.scatter([NEVER], [y], marker="x", s=60, color=SYS[sysname], zorder=3)
            ax.text(NEVER, y + 0.38, f"never ({k})", ha="center", va="bottom", fontsize=8, color=INK)
    ax.set_yticks(range(n))
    ax.set_yticklabels([c[0] for c in CASES][::-1])
    ax.set_xscale("log")
    ax.set_xlim(0.2, 2.5e4)
    ax.set_xticks([1, 10, 100, 1000, NEVER])
    ax.set_xticklabels(["1 s", "10 s", "100 s", "1000 s", "never"])
    ax.grid(axis="x", color=GRID, lw=0.6)
    ax.set_xlabel(xlabel)
    ax.set_title(title, loc="left", fontsize=11, fontweight="bold")
    ax.tick_params(length=0)


def fig_detect():
    fig, ax = plt.subplots(figsize=(9, 5.2))
    vals, never = [], []
    for _, _, rs in CASES:
        P = [A.parts(r) for r in rs]
        vals.append([p["detect"] for p in P if p["detect"] is not None])
        never.append(sum(1 for r in rs if r.get("t_fail") and not A.detected(r)))
    strip(ax, vals, never, "Detection: seconds from the failure until the client's stream ends",
          "seconds after the injected failure (log scale); dots are trials, bar is the median")
    fig.tight_layout()
    fig.savefig(f"{OUT}/detect.png")
    plt.close(fig)


def fig_C():
    fig, ax = plt.subplots(figsize=(9, 5.2))
    vals, never = [], []
    for _, _, rs in CASES:
        v, k = [], 0
        for r in rs:
            p = A.parts(r)
            c = c_valid(r, p)
            if c is not None:
                v.append(c)
            elif r in exo and p["C"] is None:
                k += 1          # placement deadlock
            elif r in lcpp and r["mode"] == "cut" and (p["C"] is None or r.get("main_restart_stuck")):
                k += 1          # hung until an operator recreated the cluster
        vals.append(v)
        never.append(k)
    strip(ax, vals, never, "C: seconds from the failure until the first token of the resubmitted request",
          "seconds after the injected failure (log scale); x marks recoveries that never completed")
    ax.text(NEVER, len(CASES) - 1 + 0.9, "", fontsize=8)
    fig.tight_layout()
    fig.savefig(f"{OUT}/recovery.png")
    plt.close(fig)


def fig_breakdown():
    """Median phases of C, stacked, for the four recovering cases."""
    def med(rs, key, fn=None):
        v = [(fn or (lambda r, p: p[key]))(r, A.parts(r)) for r in rs]
        v = [x for x in v if x is not None]
        return st.median(v) if v else 0.0

    exo_w = [r for r in exo if not is_master(r)]
    vk = [r for r in vllm if r["mode"] == "kill"]
    vp = [r for r in vllm if r["mode"] == "pause" and c_valid(r, A.parts(r)) is not None]
    lk = [r for r in lcpp if r["mode"] in ("kill", "killproc") and r.get("prompt_tokens_requested") == 33]

    rows = []
    # exo: detect | gone→ready minus load/warmup | load | warmup | prefill
    d = med(exo_w, "detect"); ld = med(exo_w, "load"); wu = med(exo_w, "warmup")
    pr = med(exo_w, "prefill"); C = med(exo_w, "C")
    rows.append(("exo, worker lost", [d, C - d - ld - wu - pr, ld, wu, pr]))
    for label, rs in (("vLLM, worker killed", vk), ("vLLM, worker hung", vp)):
        d = med(rs, "detect"); C = med(rs, "C")
        pr = med(rs, None, lambda r, p: r["t_resubmit_first_token"] - r["t_ready_again"])
        rows.append((label, [d, C - d - pr, 0, 0, pr]))
    d = med(lk, "detect"); C = med(lk, "C"); pr = med(lk, "prefill_reported")
    rows.append(("llama.cpp, rpc host killed", [d, C - d - pr, 0, 0, pr]))

    fig, ax = plt.subplots(figsize=(9, 3.6))
    names = list(PHASE)
    for i, (label, segs) in enumerate(rows):
        y = len(rows) - 1 - i
        left = 0.0
        for name, w in zip(names, segs):
            if w <= 0:
                continue
            ax.barh(y, w, left=left, height=0.55, color=PHASE[name], edgecolor="white", linewidth=1.5)
            if w > 30:
                ax.text(left + w / 2, y, f"{name}\n{w:.0f} s", ha="center", va="center", fontsize=8,
                        color="white" if name != "warmup" else INK)
            left += w
        ax.text(left + 4, y, f"C = {left:.0f} s", va="center", fontsize=9, color=INK)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([r[0] for r in rows][::-1])
    ax.set_xlim(0, 430)
    ax.set_xlabel("seconds after the failure (medians of the trials)")
    ax.grid(axis="x", color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)
    ax.set_title("Where C goes: detection, waiting and relaunch, load, warmup, prefill",
                 loc="left", fontsize=11, fontweight="bold")
    handles = [Line2D([], [], marker="s", ls="", color=PHASE[n], markersize=9, label=n) for n in names]
    ax.legend(handles=handles, loc="lower right", frameon=False, fontsize=8, ncol=5)
    fig.tight_layout()
    fig.savefig(f"{OUT}/breakdown.png")
    plt.close(fig)


def fig_client_view():
    """What the client sees in one representative trial per case: token arrivals."""
    def pick_median(rs, key="C"):
        rs = [r for r in rs if c_valid(r, A.parts(r)) is not None]
        rs.sort(key=lambda r: A.parts(r)[key])
        return rs[len(rs) // 2]

    reps = [
        ("exo, worker killed", "exo", pick_median([r for r in exo if r["mode"] == "kill" and not is_master(r)])),
        ("vLLM, worker killed", "vLLM", pick_median([r for r in vllm if r["mode"] == "kill"])),
        ("vLLM, worker paused", "vLLM", pick_median([r for r in vllm if r["mode"] == "pause"])),
        ("llama.cpp, rpc host killed", "llama.cpp", pick_median([r for r in lcpp if r["mode"] == "killproc" and r.get("prompt_tokens_requested") == 33])),
        ("llama.cpp, rpc host partitioned", "llama.cpp",
         [r for r in lcpp if r["mode"] == "cut" and r.get("t_resubmit_first_token") and not r.get("main_restart_stuck")][0]),
    ]
    fig, axes = plt.subplots(len(reps), 1, figsize=(9, 6.4))
    for ax, (label, sysname, r) in zip(axes, reps):
        t0 = r["t_request"]
        col = SYS[sysname]
        ts, acc = [], r["t_first_token"] - t0
        ts.append(acc)
        for dt in r["inter_token_s"]:
            acc += dt
            ts.append(acc)
        ts = ts[:r.get("tokens_before_end") or len(ts)]
        ax.scatter(ts, [0] * len(ts), marker="|", s=120, color=col, lw=1.2, zorder=3)
        tf = r["t_fail"] - t0
        te = r["t_stream_end"] - t0
        ax.axvline(tf, color=INK, lw=1, ls=":", zorder=2)
        ax.text(tf, 0.55, "failure", fontsize=8, ha="left", va="bottom", color=INK)
        ax.plot([tf, te], [0, 0], color=GRID, lw=6, zorder=1, solid_capstyle="butt")
        ax.scatter([te], [0], marker="x", s=60, color=INK, zorder=4)
        end_txt = "stream errors" if A.detected(r) else "harness gives up (600 s)"
        ax.text(te, -0.6, end_txt, fontsize=8, ha="center", va="top", color=INK2)
        tr = r["t_resubmit_first_token"] - t0
        ax.scatter([tr], [0], marker="|", s=160, color=col, lw=2, zorder=3)
        ax.text(tr, 0.55, f"first token of the\nresubmitted request, {tr - tf:.0f} s after the failure",
                fontsize=8, ha="right" if tr > 0.7 * tr else "left", va="bottom", color=INK)
        ax.set_xlim(-0.02 * tr, tr * 1.04)
        ax.set_ylim(-1.3, 1.6)
        ax.set_yticks([])
        ax.set_ylabel(label, rotation=0, ha="right", va="center", fontsize=9, color=INK)
        ax.spines["left"].set_visible(False)
        ax.tick_params(length=0)
        ax.grid(axis="x", color=GRID, lw=0.6)
        ax.set_axisbelow(True)
    axes[-1].set_xlabel("seconds since the request was sent (each panel has its own scale); ticks are tokens")
    axes[0].set_title("What the client sees: one median trial per case", loc="left", fontsize=11, fontweight="bold")
    fig.tight_layout()
    fig.savefig(f"{OUT}/client-view.png")
    plt.close(fig)


def fig_master_paths():
    fig, ax = plt.subplots(figsize=(9, 2.8))
    modes = [("kill", "killed"), ("pause", "paused"), ("cut", "partitioned")]
    for i, (m, lab) in enumerate(modes):
        y = len(modes) - 1 - i
        rs = [r for r in exo if r["mode"] == m and is_master(r)]
        d = [A.parts(r)["detect"] for r in rs]
        ax.axhline(y, color=GRID, lw=0.6, zorder=0)
        ax.scatter(d, [y] * len(d), s=36, color=SYS["exo"], alpha=0.6, edgecolor="white", zorder=3)
    for x, t in ((3.3, "link close\n3.3 s"), (13.2, "zenoh lease\n13.2 s"), (31.5, "heartbeat\n30–36 s")):
        ax.text(x, len(modes) - 0.45, t, ha="center", va="bottom", fontsize=8, color=INK)
    ax.set_yticks(range(len(modes)))
    ax.set_yticklabels([f"master {lab}" for _, lab in modes][::-1])
    ax.set_xlim(0, 40)
    ax.set_ylim(-0.6, len(modes) + 0.3)
    ax.set_xlabel("seconds from the failure until the client's stream ends (n = 10, 5, 5)")
    ax.grid(axis="x", color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)
    ax.set_title("exo master loss: three detection paths, chosen by chance for a crash",
                 loc="left", fontsize=11, fontweight="bold")
    fig.tight_layout()
    fig.savefig(f"{OUT}/exo-master.png")
    plt.close(fig)


def fig_prefill():
    rs = [r for r in lcpp if r["mode"] == "killproc" and r.get("resubmit_timings")]
    xs = [r["resubmit_usage"]["prompt_tokens"] for r in rs]
    ys = [r["resubmit_timings"]["prompt_ms"] / 1000 for r in rs]
    dec = st.median([A.parts(r)["decode_s_per_tok"] for r in lcpp if A.parts(r)["decode_s_per_tok"]])
    fig, ax = plt.subplots(figsize=(9, 3.8))
    grid = [50, 100, 300, 1000, 3000, 8000]
    ax.plot(grid, [g * dec for g in grid], color=INK2, lw=1.2, ls="--",
            label=f"if prefill cost the same per token as decode ({dec*1000:.0f} ms/token)")
    ax.scatter(xs, ys, s=36, color=SYS["llama.cpp"], edgecolor="white", zorder=3, label="measured prefill (llama-server timings.prompt_ms)")
    for n in sorted(set(xs)):
        y = st.median([b for a, b in zip(xs, ys) if a == n])
        ax.text(n * 1.12, y, f"{y/n*1000:.1f} ms/token", ha="left", va="center", fontsize=8, color=INK)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("prompt tokens")
    ax.set_ylabel("prefill seconds")
    ax.grid(color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    ax.set_title("llama.cpp RPC: prefill is 7–10× cheaper per token than decode on this CPU",
                 loc="left", fontsize=11, fontweight="bold")
    fig.tight_layout()
    fig.savefig(f"{OUT}/prefill.png")
    plt.close(fig)


def fig_model():
    """Closed forms with the measured constants (churnsim.measured in the public repo)."""
    N, D = 15, 30 * 3600
    life = [2 ** (k / 4) for k in range(0, 44)]       # 1 day .. ~1800 days
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 3.8))
    P = [math.exp(-N * D / (d * 86400)) for d in life]
    a1.plot(life, P, color=SYS["exo"], lw=2)
    for p, lab in ((0.5, "50%"), (0.9, "90%")):
        d = N * D / (-math.log(p)) / 86400
        a1.plot([d, d], [0, p], color=GRID, lw=1)
        a1.text(d, p + 0.03, f"{lab}: {d:.0f} d", ha="center", fontsize=8, color=INK)
    a1.set_xscale("log")
    a1.set_xlabel("mean device lifetime (days)")
    a1.set_ylabel("P(window completes with no failure)")
    a1.set_ylim(0, 1.05)
    a1.set_title("Restart (all three systems):\n15 devices, a 30 h window", loc="left", fontsize=10, fontweight="bold")
    for C, lab, col in ((275, "C = 275 s (exo)", SYS["exo"]), (67, "C = 67 s (vLLM crash)", SYS["vLLM"]), (14, "C = 14 s (llama.cpp crash)", SYS["llama.cpp"])):
        extra = [N * D / (d * 86400) * C / 60 for d in life]
        a2.plot(life, extra, color=col, lw=2, label=lab)
    a2.set_xscale("log"); a2.set_yscale("log")
    a2.set_xlabel("mean device lifetime (days)")
    a2.set_ylabel("expected recovery time added (minutes)")
    a2.set_title("If the same stacks had Petals-style replay:\nexpected time lost on the same window", loc="left", fontsize=10, fontweight="bold")
    a2.legend(frameon=False, fontsize=8)
    for a in (a1, a2):
        a.grid(color=GRID, lw=0.6); a.set_axisbelow(True); a.tick_params(length=0)
    fig.tight_layout()
    fig.savefig(f"{OUT}/model.png")
    plt.close(fig)


if __name__ == "__main__":
    fig_detect()
    fig_C()
    fig_breakdown()
    fig_client_view()
    fig_master_paths()
    fig_prefill()
    fig_model()
    print("wrote", sorted(os.listdir(OUT)))
