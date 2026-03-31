#!/usr/bin/env python3
"""Plot per-tx-type latency from BenchBase TPC-C results.

Usage:
    python3 plot_latency.py --root ansible/result/<run_id>
"""
import argparse
import csv
import tarfile
from pathlib import Path
from io import TextIOWrapper

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


TX_TYPES = ["NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"]
TX_COLORS = {
    "NewOrder": "#1f77b4",
    "Payment": "#ff7f0e",
    "OrderStatus": "#2ca02c",
    "Delivery": "#d62728",
    "StockLevel": "#9467bd",
}
TX_MARKERS = {
    "NewOrder": "o",
    "Payment": "s",
    "OrderStatus": "^",
    "Delivery": "D",
    "StockLevel": "v",
}


def find_config_root(root: Path) -> Path:
    if (root / "bench").is_dir():
        return root
    configs = [p for p in root.iterdir() if p.is_dir() and (p / "bench").is_dir()]
    if configs:
        return max(configs, key=lambda p: p.stat().st_mtime)
    return root


def extract_bench_data(config_root: Path):
    """Extract per-terminal per-tx-type latency from bench archive."""
    bench_dir = config_root / "bench"
    tgz_files = list(bench_dir.glob("*/bench-*.tgz"))
    if not tgz_files:
        return {}

    results = {}

    for tgz_path in tgz_files:
        with tarfile.open(tgz_path) as tar:
            for member in tar.getmembers():
                parts = member.name.split("/")
                if len(parts) < 3:
                    continue
                dir_name = parts[1]
                file_name = parts[2]

                if not dir_name.startswith("execute_"):
                    continue
                terminals = int(dir_name.split("_")[1])

                for tx in TX_TYPES:
                    if file_name.endswith(f".results.{tx}.csv"):
                        f = tar.extractfile(member)
                        if f is None:
                            continue
                        reader = csv.DictReader(TextIOWrapper(f))
                        rows = list(reader)
                        if not rows:
                            continue

                        data_rows = rows[:-1] if len(rows) > 2 else rows
                        p50_lat = np.mean([float(r["Median Latency (millisecond)"]) for r in data_rows])
                        p95_lat = np.mean([float(r["95th Percentile Latency (millisecond)"]) for r in data_rows])
                        p99_lat = np.mean([float(r["99th Percentile Latency (millisecond)"]) for r in data_rows])

                        results.setdefault(terminals, {})[tx] = {
                            "p50": p50_lat,
                            "p95": p95_lat,
                            "p99": p99_lat,
                        }

    return results


def plot_latency(results, config_root: Path, output_dir: Path):
    if not results:
        print("No data to plot")
        return

    terminals = sorted(results.keys())
    machine_spec = config_root.name

    # --- Latency distribution (p50/p95/p99), shared y-axis ---
    fig, axes = plt.subplots(1, len(TX_TYPES), figsize=(20, 5), sharey=True)

    for i, tx in enumerate(TX_TYPES):
        ax = axes[i]
        p50 = [results[t].get(tx, {}).get("p50", 0) for t in terminals]
        p95 = [results[t].get(tx, {}).get("p95", 0) for t in terminals]
        p99 = [results[t].get(tx, {}).get("p99", 0) for t in terminals]
        color = TX_COLORS[tx]

        ax.fill_between(terminals, p50, p95, alpha=0.15, color=color)
        ax.fill_between(terminals, p95, p99, alpha=0.08, color=color)
        ax.plot(terminals, p50, "o-", color=color, linewidth=2, markersize=4, label="p50")
        ax.plot(terminals, p95, "^-", color=color, linewidth=1, markersize=3, alpha=0.7, label="p95")
        ax.plot(terminals, p99, "s--", color=color, linewidth=1, markersize=3, alpha=0.5, label="p99")

        ax.set_title(tx, fontweight="bold")
        ax.set_xlabel("Terminals")
        if i == 0:
            ax.set_ylabel("Latency (ms)")
        ax.legend(fontsize=7, loc="upper left")
        ax.grid(True, alpha=0.2)

    fig.suptitle(f"TPC-C Latency Distribution\n{machine_spec}", y=1.02)
    path2 = output_dir / f"tpcc_latency-{machine_spec}.png"
    plt.tight_layout()
    plt.savefig(path2, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"wrote {path2}")


def main():
    parser = argparse.ArgumentParser(description="Plot TPC-C per-tx-type latency")
    parser.add_argument("--root", type=Path, required=True, help="Result root directory")
    args = parser.parse_args()

    config_root = find_config_root(args.root)
    print(f"using config dir: {config_root}")

    results = extract_bench_data(config_root)
    if not results:
        print("No benchmark data found")
        return

    output_dir = config_root / "_plot"
    output_dir.mkdir(exist_ok=True)

    plot_latency(results, config_root, output_dir)


if __name__ == "__main__":
    main()
