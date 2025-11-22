#!/usr/bin/env python3
"""
Plot CPU frequency logs captured by run_local_with_pidstat.sh.

Usage:
  python scripts/plot_freq.py \
    --run-dir bench/results/_run_local/20251122_192312 \
    --terminals 16 20 24 28 \
    --output freq_plot.png

Notes:
  - Expects files named freq_log_t<term>.txt under --run-dir.
  - Each freq_log_t*.txt line: "<timestamp> <freq0> <freq1> ...".
  - Plots one subplot per terminals value; each subplot shows per-CPU freq.
"""

import argparse
import datetime as dt
import glob
import os
from typing import List, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot CPU frequency logs.")
    p.add_argument(
        "--run-dir",
        required=True,
        help="Directory containing freq_log_t*.txt (e.g. bench/results/_run_local/<run_id>)",
    )
    p.add_argument(
        "--terminals",
        nargs="+",
        type=int,
        default=None,
        help="Specific terminal counts to plot (e.g. 16 20 24 28). Defaults to all freq_log_t*.txt found.",
    )
    p.add_argument(
        "--output",
        default="freq_plot.png",
        help="Output image path (default: freq_plot.png)",
    )
    p.add_argument(
        "--downsample",
        type=int,
        default=1,
        help="Plot every Nth sample to keep lines readable (default: 1 = all samples)",
    )
    p.add_argument(
        "--relative-time",
        action="store_true",
        help="Plot time axis as seconds from the first sample of each file instead of wall clock",
    )
    return p.parse_args()


def load_freq_log(path: str, downsample: int) -> Tuple[List[dt.datetime], List[List[float]]]:
    timestamps: List[dt.datetime] = []
    freqs: List[List[float]] = []
    with open(path, "r") as f:
        for idx, line in enumerate(f):
            if downsample > 1 and (idx % downsample != 0):
                continue
            parts = line.strip().split()
            if len(parts) < 2:
                continue
            ts_str = " ".join(parts[0:2])
            try:
                ts = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            vals = []
            for freq_str in parts[2:]:
                try:
                    vals.append(float(freq_str) / 1000.0)  # kHz -> MHz
                except ValueError:
                    vals.append(float("nan"))
            timestamps.append(ts)
            freqs.append(vals)
    return timestamps, freqs


def main() -> None:
    args = parse_args()
    term_files = {}

    if args.terminals:
        for t in args.terminals:
            pattern = os.path.join(args.run_dir, f"freq_log_t{t}.txt")
            matches = glob.glob(pattern)
            if matches:
                term_files[t] = matches[0]
    else:
        for path in glob.glob(os.path.join(args.run_dir, "freq_log_t*.txt")):
            base = os.path.basename(path)
            # Extract number after 't'
            try:
                t = int(base.split("freq_log_t")[1].split(".")[0])
                term_files[t] = path
            except Exception:
                continue

    if not term_files:
        raise SystemExit("No freq_log_t*.txt files found.")

    sorted_terms = sorted(term_files.keys())
    fig, axes = plt.subplots(len(sorted_terms), 1, figsize=(12, 3 * len(sorted_terms)), sharex=True)
    if len(sorted_terms) == 1:
        axes = [axes]

    for ax, term in zip(axes, sorted_terms):
        path = term_files[term]
        ts, freqs = load_freq_log(path, args.downsample)
        if not ts or not freqs:
            ax.set_title(f"{term} terminals (no data)")
            continue
        if args.relative_time:
            base = ts[0]
            x_vals = [(t - base).total_seconds() for t in ts]
        else:
            x_vals = ts
        # Transpose to per-CPU series
        n_cpu = max(len(row) for row in freqs)
        series = [[] for _ in range(n_cpu)]
        for row in freqs:
            # pad if needed
            padded = row + [float("nan")] * (n_cpu - len(row))
            for i, val in enumerate(padded):
                series[i].append(val)
        for cpu_id, values in enumerate(series):
            ax.plot(x_vals, values, linewidth=0.6, alpha=0.6, label=f"CPU{cpu_id}")
        ax.set_ylabel("MHz")
        ax.set_title(f"Terminals={term}")
        ax.grid(True, linestyle="--", alpha=0.3)
        if not args.relative_time:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    axes[-1].set_xlabel("Time" if not args.relative_time else "Seconds from start")
    # Hide legend to avoid clutter; enable if needed
    # axes[0].legend(ncol=8, fontsize=6)
    fig.tight_layout()
    fig.savefig(args.output, dpi=200)
    print(f"Saved plot to {args.output}")


if __name__ == "__main__":
    main()
