#!/usr/bin/env python3
"""
Plot TPC-H per-query latency from BenchBase raw.csv files.

Reads bench result tgz archives, extracts raw.csv, and generates:
  1. Bar chart of average latency per query (Q1-Q22)
  2. Boxplot of latency distribution per query (when multiple data points exist)

Usage:
  python3 py/plot_tpch.py                          # auto-detect latest result
  python3 py/plot_tpch.py --root result/<config>   # specific config dir
"""
import argparse
import csv
import io
import sys
import tarfile
from collections import defaultdict
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np


ALL_QUERIES = [f"Q{i}" for i in range(1, 23)]


def select_config_root(root: Path) -> Path:
    """Find the most recent config directory under result/."""
    if (root / "bench").is_dir():
        return root
    if not root.exists():
        return root
    configs = [p for p in root.iterdir() if p.is_dir() and (p / "bench").is_dir()]
    if not configs:
        return root
    return max(configs, key=lambda p: p.stat().st_mtime)


def extract_raw_csvs(config_root: Path) -> list[dict]:
    """Extract raw.csv data from bench tgz archives.

    Returns list of dicts: {query: str, latency_us: int, worker: int, terminal_count: str}
    """
    bench_dir = config_root / "bench"
    if not bench_dir.exists():
        return []

    records = []
    for tgz_path in sorted(bench_dir.rglob("*.tgz")):
        try:
            with tarfile.open(tgz_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if not member.name.endswith(".raw.csv"):
                        continue
                    # Extract terminal count from path: .../execute_<N>/...raw.csv
                    parts = Path(member.name).parts
                    term_count = "?"
                    for part in parts:
                        if part.startswith("execute_"):
                            term_count = part.replace("execute_", "")
                            break

                    f = tar.extractfile(member)
                    if f is None:
                        continue
                    text = f.read().decode("utf-8", errors="replace")
                    reader = csv.reader(io.StringIO(text))
                    for row in reader:
                        if len(row) < 4:
                            continue
                        try:
                            query = row[1].strip()
                            latency_us = int(row[3].strip())
                            worker = int(row[4].strip()) if len(row) > 4 else 0
                        except (ValueError, IndexError):
                            continue
                        if query.startswith("Q"):
                            records.append({
                                "query": query,
                                "latency_us": latency_us,
                                "worker": worker,
                                "terminal_count": term_count,
                            })
        except (tarfile.TarError, OSError) as e:
            print(f"  warning: could not read {tgz_path}: {e}", file=sys.stderr)

    return records


def plot_bar(query_latencies: dict[str, list[float]], output: Path, title: str):
    """Bar chart of average latency per query."""
    queries = []
    avgs = []
    for q in ALL_QUERIES:
        if q in query_latencies and query_latencies[q]:
            queries.append(q)
            avgs.append(np.mean(query_latencies[q]))

    if not queries:
        print("no data for bar chart", file=sys.stderr)
        return

    fig, ax = plt.subplots(figsize=(14, 6))

    # Color by latency magnitude
    max_lat = max(avgs) if avgs else 1
    colors = []
    for v in avgs:
        ratio = v / max_lat if max_lat > 0 else 0
        if ratio < 0.2:
            colors.append("#2ecc71")  # green
        elif ratio < 0.5:
            colors.append("#f39c12")  # orange
        else:
            colors.append("#e74c3c")  # red

    bars = ax.bar(range(len(queries)), avgs, color=colors, edgecolor="white", linewidth=0.5)

    # Add value labels on bars
    for bar, val in zip(bars, avgs):
        if val >= 1000:
            label = f"{val / 1000:.1f}s"
        else:
            label = f"{val:.0f}ms"
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                label, ha="center", va="bottom", fontsize=8)

    ax.set_xticks(range(len(queries)))
    ax.set_xticklabels(queries, fontsize=9)
    ax.set_ylabel("Average Latency (ms)")
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)
    print(f"wrote {output}")


def plot_box(query_latencies: dict[str, list[float]], output: Path, title: str):
    """Boxplot of latency distribution per query."""
    queries = []
    data = []
    for q in ALL_QUERIES:
        if q in query_latencies and len(query_latencies[q]) >= 2:
            queries.append(q)
            data.append(query_latencies[q])

    if not queries:
        print("not enough data points for boxplot (need >=2 per query)", file=sys.stderr)
        return

    fig, ax = plt.subplots(figsize=(14, 6))
    bp = ax.boxplot(data, patch_artist=True, showfliers=True,
                    flierprops=dict(marker="o", markersize=3, alpha=0.5))

    # Color boxes by median
    medians = [np.median(d) for d in data]
    max_med = max(medians) if medians else 1
    for i, (patch, med) in enumerate(zip(bp["boxes"], medians)):
        ratio = med / max_med if max_med > 0 else 0
        if ratio < 0.2:
            patch.set_facecolor("#2ecc71")
        elif ratio < 0.5:
            patch.set_facecolor("#f39c12")
        else:
            patch.set_facecolor("#e74c3c")
        patch.set_alpha(0.7)

    ax.set_xticks(range(1, len(queries) + 1))
    ax.set_xticklabels(queries, fontsize=9)
    ax.set_ylabel("Latency (ms)")
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)
    print(f"wrote {output}")


def write_summary_csv(query_latencies: dict[str, list[float]], output: Path):
    """Write per-query summary statistics CSV."""
    with output.open("w") as f:
        f.write("query,count,avg_ms,min_ms,p50_ms,p95_ms,max_ms\n")
        for q in ALL_QUERIES:
            if q not in query_latencies or not query_latencies[q]:
                continue
            vals = sorted(query_latencies[q])
            n = len(vals)
            avg = np.mean(vals)
            p50 = np.percentile(vals, 50)
            p95 = np.percentile(vals, 95)
            f.write(f"{q},{n},{avg:.1f},{vals[0]:.1f},{p50:.1f},{p95:.1f},{vals[-1]:.1f}\n")
    print(f"wrote {output}")


def main():
    parser = argparse.ArgumentParser(description="Plot TPC-H per-query latency")
    parser.add_argument("--root", type=Path, default=None, help="config dir or result/ root")
    parser.add_argument("--output-dir", type=Path, default=None, help="output directory for plots")
    parser.add_argument("--title", type=str, default=None, help="plot title prefix")
    args = parser.parse_args()

    if args.root is None:
        base = Path(__file__).resolve().parent.parent
        args.root = base / "result"

    config_root = select_config_root(args.root)
    if not config_root.exists():
        print(f"no data found under {args.root}", file=sys.stderr)
        return 1

    config_name = config_root.name
    # Try to read machine_spec from meta.txt for a better title
    meta_file = config_root / "meta.txt"
    meta_spec = None
    if meta_file.exists():
        for line in meta_file.read_text().splitlines():
            if line.startswith("machine_spec="):
                meta_spec = line.split("=", 1)[1].strip()
                break
    title_prefix = args.title or (meta_spec or config_name).replace("_", " ")

    print(f"using config dir: {config_root}")
    records = extract_raw_csvs(config_root)
    if not records:
        print(f"no TPC-H raw.csv data found in {config_root}/bench/", file=sys.stderr)
        return 1

    print(f"  found {len(records)} query records")

    # Group latencies by query (convert us -> ms)
    query_latencies: dict[str, list[float]] = defaultdict(list)
    for r in records:
        query_latencies[r["query"]].append(r["latency_us"] / 1000.0)

    # Output directory
    out_dir = args.output_dir or config_root / "_plot"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Write CSV summary
    write_summary_csv(query_latencies, out_dir / "tpch_query_summary.csv")

    # Bar chart (average latency)
    plot_bar(query_latencies, out_dir / f"tpch_bar-{config_name}.png",
             f"{title_prefix}\nPer-Query Average Latency")

    # Boxplot (distribution) — only if enough data points
    has_multi = any(len(v) >= 2 for v in query_latencies.values())
    if has_multi:
        plot_box(query_latencies, out_dir / f"tpch_box-{config_name}.png",
                 f"{title_prefix}\nPer-Query Latency Distribution")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
