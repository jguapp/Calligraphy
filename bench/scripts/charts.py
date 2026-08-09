#!/usr/bin/env python3
"""Renders the README charts from committed benchmark results.

Reads bench/results/*.json (the real runs -- nothing here is typed in by
hand) and emits light + dark SVGs into .github/assets/, which the README
selects between with a <picture> element so the chart matches GitHub's
theme.

Design rules applied (and why they look the way they do): two categorical
series maximum, hues validated for CVD separation on both surfaces; 2px
lines with 8px markers; value labels on ends only; no dual axes ever.
"""

import glob
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

MODES = {
    "light": {
        "surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e",
        "grid": "#e4e2dc", "blue": "#2a78d6", "orange": "#eb6834",
    },
    "dark": {
        "surface": "#1a1a19", "text": "#ffffff", "muted": "#c3c2b7",
        "grid": "#33322f", "blue": "#3987e5", "orange": "#d95926",
    },
}

OUT = os.path.join(os.path.dirname(__file__), "..", "..", ".github", "assets")


def load(name_prefix):
    """Newest result whose name starts with the prefix."""
    hits = {}
    for path in glob.glob(os.path.join(os.path.dirname(__file__), "..", "results", "*.json")):
        with open(path) as f:
            r = json.load(f)
        if r["name"].startswith(name_prefix):
            hits[r["name"]] = r  # glob sorts oldest-first; later overwrites
    return hits


def style(ax, m):
    ax.set_facecolor(m["surface"])
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(m["grid"])
    ax.tick_params(colors=m["muted"], labelsize=10)
    ax.yaxis.grid(True, color=m["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def sweep_chart(mode, m):
    runs = load("sweep-")
    workers = [1, 2, 4, 7]
    io = [runs[f"sweep-io-{n}w"]["run"]["throughputJobsPerSec"] for n in workers]
    cpu = [runs[f"sweep-cpu-{n}w"]["run"]["throughputJobsPerSec"] for n in workers]

    fig, ax = plt.subplots(figsize=(8.6, 4.6), dpi=100)
    fig.patch.set_facecolor(m["surface"])
    style(ax, m)

    ax.axvline(4, color=m["grid"], linewidth=1.4, linestyle=(0, (4, 4)))
    ax.annotate("4 physical cores", xy=(4, 8), color=m["muted"], fontsize=9,
                ha="center", va="bottom", xytext=(4, 12))

    ax.plot(workers, io, color=m["blue"], linewidth=2, marker="o", markersize=8,
            markerfacecolor=m["blue"], markeredgecolor=m["surface"], markeredgewidth=2)
    ax.plot(workers, cpu, color=m["orange"], linewidth=2, marker="o", markersize=8,
            markerfacecolor=m["orange"], markeredgecolor=m["surface"], markeredgewidth=2)

    # Direct labels at the line ends; values only at the ends (selective).
    ax.annotate(f"I/O-bound · {io[-1]:.0f}/s", xy=(workers[-1], io[-1]),
                xytext=(8, 0), textcoords="offset points",
                color=m["text"], fontsize=10.5, fontweight="bold", va="center")
    ax.annotate(f"CPU-bound · {cpu[-1]:.0f}/s", xy=(workers[-1], cpu[-1]),
                xytext=(8, 0), textcoords="offset points",
                color=m["text"], fontsize=10.5, fontweight="bold", va="center")
    ax.annotate(f"{io[0]:.0f}/s", xy=(workers[0], io[0]), xytext=(0, 10),
                textcoords="offset points", color=m["muted"], fontsize=9, ha="center")
    ax.annotate(f"{cpu[0]:.0f}/s", xy=(workers[0], cpu[0]), xytext=(0, -16),
                textcoords="offset points", color=m["muted"], fontsize=9, ha="center")

    ax.set_xticks(workers)
    ax.set_xlim(0.6, 9.4)
    ax.set_ylim(0, max(io) * 1.14)
    ax.set_xlabel("worker processes (concurrency 4 each)", color=m["muted"], fontsize=10)
    ax.set_ylabel("completed jobs / second", color=m["muted"], fontsize=10)
    ax.set_title("Throughput vs worker count — 3,000 I/O-bound and 1,500 CPU-bound jobs, 4-core host",
                 color=m["text"], fontsize=11.5, loc="left", pad=14)

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f"bench-sweep-{mode}.svg"),
                facecolor=m["surface"], bbox_inches="tight")
    plt.close(fig)


def ablation_chart(mode, m):
    runs = load("ablation-v")
    steps = [
        ("v0  serial baseline\nconc 1 · pools 1 · sync writes", "ablation-v0-baseline"),
        ("v1  + connection pooling", "ablation-v1-pooling"),
        ("v2  + concurrency 8, batched fetch", "ablation-v2-concurrency"),
        ("v3  + batched writes", "ablation-v3-batching"),
    ]
    labels = [s[0] for s in steps]
    vals = [runs[s[1]]["run"]["throughputJobsPerSec"] for s in steps]

    fig, ax = plt.subplots(figsize=(8.6, 4.2), dpi=100)
    fig.patch.set_facecolor(m["surface"])
    style(ax, m)
    ax.xaxis.grid(True, color=m["grid"], linewidth=0.8)
    ax.yaxis.grid(False)

    y = range(len(steps))[::-1]
    ax.barh(list(y), vals, height=0.52, color=m["blue"], zorder=3)
    for yi, v in zip(y, vals):
        ax.annotate(f"{v:.0f} jobs/s", xy=(v, yi), xytext=(7, 0),
                    textcoords="offset points", va="center",
                    color=m["text"], fontsize=10.5, fontweight="bold")

    ax.set_yticks(list(y))
    ax.set_yticklabels(labels, color=m["text"], fontsize=10)
    ax.set_xlim(0, max(vals) * 1.22)
    ax.set_xlabel("completed jobs / second — 1,000 × 50–70ms I/O-bound jobs, ONE worker process",
                  color=m["muted"], fontsize=10)
    ax.set_title("What each optimization was actually worth (measured one lever at a time)",
                 color=m["text"], fontsize=11.5, loc="left", pad=14)

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f"bench-ablation-{mode}.svg"),
                facecolor=m["surface"], bbox_inches="tight")
    plt.close(fig)


def main():
    os.makedirs(OUT, exist_ok=True)
    for mode, m in MODES.items():
        sweep_chart(mode, m)
        ablation_chart(mode, m)
    print("wrote", sorted(os.listdir(OUT)), file=sys.stderr)


if __name__ == "__main__":
    main()
