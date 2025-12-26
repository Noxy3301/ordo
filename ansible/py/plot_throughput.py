#!/usr/bin/env python3
import argparse
from pathlib import Path
import sys
from typing import Optional

import matplotlib.pyplot as plt


def select_config_root(root: Path) -> Path:
    if root.name == "throughput":
        return root.parent
    if (root / "throughput").is_dir():
        return root
    if not root.exists():
        return root
    configs = [path for path in root.iterdir() if path.is_dir()]
    if not configs:
        return root

    def latest_mtime(path: Path) -> float:
        throughput_root = path / "throughput"
        logs = list(throughput_root.glob("throughput_raw.csv"))
        if not logs:
            return path.stat().st_mtime
        return max(log.stat().st_mtime for log in logs)

    return max(configs, key=latest_mtime)


def parse_throughput(csv_path: Path):
    rows = []
    for raw in csv_path.read_text().splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 3:
            continue
        try:
            terminal = int(parts[1])
            throughput = float(parts[2])
        except ValueError:
            continue
        rows.append((parts[0], terminal, throughput))
    return rows


def aggregate(rows):
    totals = {}
    hosts = {}
    for host, terminal, throughput in rows:
        totals[terminal] = totals.get(terminal, 0.0) + throughput
        hosts.setdefault(terminal, set()).add(host)
    summary = []
    for terminal in sorted(totals):
        host_count = len(hosts[terminal])
        total_terminals = terminal * host_count
        summary.append((total_terminals, totals[terminal], host_count, terminal))
    return summary


def write_summary_csv(path: Path, summary):
    with path.open("w") as f:
        f.write("total_terminals,total_throughput,hosts,terminals_per_host\n")
        for total_terminals, total_throughput, host_count, terminal in summary:
            f.write(f"{total_terminals},{total_throughput},{host_count},{terminal}\n")


def plot_summary(summary, output: Path, title: Optional[str]):
    x = [item[0] for item in summary]
    y = [item[1] for item in summary]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(x, y, marker="o", linewidth=2)
    ax.set_xlabel("total terminals")
    ax.set_ylabel("total throughput")
    if title:
        ax.set_title(title)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output, dpi=150)


def main():
    parser = argparse.ArgumentParser(description="Plot throughput from BenchBase summaries")
    parser.add_argument("--root", type=Path, default=None, help="root directory of result or a config/throughput dir")
    parser.add_argument("--input", type=Path, default=None, help="path to throughput_raw.csv")
    parser.add_argument("--output", type=Path, default=None, help="output image path")
    parser.add_argument("--title", type=str, default=None, help="plot title")
    args = parser.parse_args()

    if args.input is None:
        if args.root is None:
            base = Path(__file__).resolve().parent.parent
            args.root = base / "result"
        config_root = select_config_root(args.root)
        if not config_root.exists():
            print(f"no data found under {args.root}", file=sys.stderr)
            return 1
        throughput_root = config_root / "throughput"
        if not throughput_root.exists():
            print(f"no throughput data under {config_root}", file=sys.stderr)
            return 1
        csv_path = throughput_root / "throughput_raw.csv"
        if not csv_path.exists():
            print(f"missing {csv_path}", file=sys.stderr)
            return 1
    else:
        csv_path = args.input
        throughput_root = csv_path.parent
        config_root = throughput_root.parent if throughput_root.name == "throughput" else throughput_root

    config_name = config_root.name
    if args.title is None:
        args.title = config_name.replace("_", " ")

    summary = aggregate(parse_throughput(csv_path))
    if not summary:
        print(f"no throughput data in {csv_path}", file=sys.stderr)
        return 1

    write_summary_csv(throughput_root / "throughput_sum.csv", summary)

    if args.output is None:
        args.output = throughput_root / f"throughput_plot-{config_name}.png"

    args.output.parent.mkdir(parents=True, exist_ok=True)
    plot_summary(summary, args.output, args.title)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
