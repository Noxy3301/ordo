#!/usr/bin/env python3
"""
Plot frequency timelines alongside throughput-vs-terminal.

Example:
  python scripts/plot_freq_and_tp.py \
    --run-dir bench/results/_run_local/20251122_192312 \
    --terminals 20 21 22 23 24 25 26 27 28 \
    --summary summary.csv \
    --relative-time \
    --output freq_tp.png
"""

import argparse
import csv
import datetime as dt
import glob
import os
from typing import Dict, List, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import gridspec
from matplotlib.ticker import MaxNLocator
from PIL import Image


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot freq timelines + throughput vs terminals.")
    p.add_argument(
        "--run-dir",
        required=False,
        default=None,
        help="Directory containing freq_log_t*.txt and summary.csv (default: latest under bench/results/_run_local)",
    )
    p.add_argument(
        "--terminals",
        nargs="+",
        type=int,
        default=None,
        help="Terminals to plot (default: infer from freq_log_t*.txt)",
    )
    p.add_argument(
        "--summary",
        default=None,
        help="Path to summary CSV (default: <run-dir>/summary.csv)",
    )
    p.add_argument(
        "--output",
        default="freq_tp.png",
        help="Combined output image path",
    )
    p.add_argument(
        "--downsample",
        type=int,
        default=1,
        help="Plot every Nth sample from freq logs (default: 1)",
    )
    p.add_argument(
        "--relative-time",
        action="store_true",
        help="Use seconds from first sample instead of wall clock on freq plots",
    )
    p.add_argument(
        "--alpha",
        type=float,
        default=0.5,
        help="Line transparency for per-CPU plots (default: 0.5)",
    )
    return p.parse_args()


def load_freq_log(path: str, downsample: int) -> Tuple[List[dt.datetime], List[List[float]]]:
    ts_list: List[dt.datetime] = []
    freqs: List[List[float]] = []
    with open(path, "r") as f:
        for idx, line in enumerate(f):
            if downsample > 1 and idx % downsample != 0:
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
            ts_list.append(ts)
            freqs.append(vals)
    return ts_list, freqs


def collect_term_files(run_dir: str, terminals: List[int] = None) -> Dict[int, str]:
    term_files: Dict[int, str] = {}
    if terminals:
        for t in terminals:
            path = os.path.join(run_dir, f"freq_log_t{t}.txt")
            if os.path.exists(path):
                term_files[t] = path
    else:
        for path in glob.glob(os.path.join(run_dir, "freq_log_t*.txt")):
            base = os.path.basename(path)
            try:
                t = int(base.split("freq_log_t")[1].split(".")[0])
                term_files[t] = path
            except Exception:
                continue
    return term_files


def load_summary(summary_path: str) -> Tuple[List[int], List[float]]:
    terms: List[int] = []
    tps: List[float] = []
    with open(summary_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                term = int(row["Terminals"])
                tp = float(row["Throughput(req/s)"])
            except Exception:
                continue
            terms.append(term)
            tps.append(tp)
    return terms, tps


def main() -> None:
    args = parse_args()
    run_dir = args.run_dir
    if run_dir is None:
        candidates = sorted(
            glob.glob(os.path.join("bench", "results", "_run_local", "*")),
            key=os.path.getmtime,
        )
        if not candidates:
            raise SystemExit("No run directories found under bench/results/_run_local")
        run_dir = candidates[-1]
    summary_path = args.summary or os.path.join(run_dir, "summary.csv")
    term_files = collect_term_files(run_dir, args.terminals)
    if not term_files:
        raise SystemExit("No freq_log_t*.txt found.")

    # ---------- Frequency figure ----------
    freq_fig = plt.figure(figsize=(12, 3 * len(term_files)))
    gs = gridspec.GridSpec(len(term_files), 1)
    sorted_terms = sorted(term_files.keys())
    for idx, term in enumerate(sorted_terms):
        ax = freq_fig.add_subplot(gs[idx, 0])
        ts, freqs = load_freq_log(term_files[term], args.downsample)
        if not ts or not freqs:
            ax.set_title(f"Terminals={term} (no data)")
            ax.axis("off")
            continue
        if args.relative_time:
            base = ts[0]
            x_vals = [(t - base).total_seconds() for t in ts]
        else:
            x_vals = ts
        n_cpu = max(len(row) for row in freqs)
        series = [[] for _ in range(n_cpu)]
        for row in freqs:
            padded = row + [float("nan")] * (n_cpu - len(row))
            for i, val in enumerate(padded):
                series[i].append(val)
        # Plot per-CPU lines
        for cpu_id, vals in enumerate(series):
            ax.plot(x_vals, vals, linewidth=0.6, alpha=args.alpha)
        # Plot per-sample average across CPUs
        avg_vals = []
        for i in range(len(freqs)):
            row = freqs[i]
            if not row:
                avg_vals.append(float("nan"))
            else:
                avg_vals.append(sum(row) / len(row))
        ax.plot(x_vals, avg_vals, color="red", linewidth=1.5, alpha=0.9, label="avg")
        ax.set_ylabel("MHz")
        ax.set_title(f"Terminals={term}")
        ax.grid(True, linestyle="--", alpha=0.3)
        if not args.relative_time:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        if idx == len(sorted_terms) - 1:
            ax.set_xlabel("Time" if not args.relative_time else "Seconds from start")
    freq_fig.tight_layout()

    # ---------- Throughput figure ----------
    # Throughput figure: larger square-ish base to preserve text size after resizing
    tp_fig, ax_tp = plt.subplots(figsize=(12, 12))
    if os.path.exists(summary_path):
        terms, tps = load_summary(summary_path)
        ax_tp.plot(terms, tps, marker="o")
        ax_tp.set_title("Throughput vs terminals")
        ax_tp.set_xlabel("Terminals")
        ax_tp.set_ylabel("Throughput (req/s)")
        ax_tp.grid(True, linestyle="--", alpha=0.3)
        if terms:
            t_min, t_max = min(terms), max(terms)
            ax_tp.set_xlim(left=t_min - 1, right=t_max + 1)
            ax_tp.xaxis.set_major_locator(MaxNLocator(integer=True))
    else:
        ax_tp.set_title("Throughput (summary not found)")
        ax_tp.axis("off")
    tp_fig.tight_layout()

    # Save interim images
    base, ext = os.path.splitext(args.output)
    freq_path = f"{base}_freq{ext or '.png'}"
    tp_path = f"{base}_tp{ext or '.png'}"
    freq_fig.savefig(freq_path, dpi=200)
    tp_fig.savefig(tp_path, dpi=200)
    plt.close(freq_fig)
    plt.close(tp_fig)

    # Combine side-by-side
    left = Image.open(freq_path)
    right = Image.open(tp_path)
    # Resize throughput image to match height of frequency image
    if right.height != left.height:
        new_w = int(right.width * (left.height / right.height))
        right = right.resize((new_w, left.height), Image.LANCZOS)
    max_h = max(left.height, right.height)
    total_w = left.width + right.width
    combined = Image.new("RGB", (total_w, max_h), (255, 255, 255))
    combined.paste(left, (0, 0))
    combined.paste(right, (left.width, 0))
    combined.save(args.output)
    print(f"Saved combined plot to {args.output}")
    print(f"  freq only : {freq_path}")
    print(f"  tp only   : {tp_path}")


if __name__ == "__main__":
    main()
