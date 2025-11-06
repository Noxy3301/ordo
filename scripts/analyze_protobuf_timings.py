#!/usr/bin/env python3
"""
簡易解析スクリプト

LineairDB の protobuf_timing.log を読み込み、
メッセージ種別ごとに serialize / deserialize / send / recv / roundtrip の
統計値（count, avg, p50, p90, p95, max）を表示します。
"""

from __future__ import annotations

import argparse
import collections
import statistics
from pathlib import Path
from typing import Dict, List


TimingSample = Dict[str, float]


def parse_line(line: str) -> TimingSample | None:
    if not line.strip():
        return None
    parts = line.strip().split()
    sample: TimingSample = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key == "message":
            sample[key] = value
            continue
        if key == "source":
            sample[key] = value
            continue
        try:
            sample[key] = float(value)
        except ValueError:
            # 念のため parse_ok を数値扱い
            try:
                sample[key] = float(value.rstrip(","))
            except ValueError:
                pass
    return sample if "message" in sample else None


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return d0 + d1


def summarize(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {name: 0.0 for name in ("count", "avg", "p50", "p90", "p95", "max")}

    sorted_samples = sorted(samples)
    return {
        "count": float(len(samples)),
        "avg": statistics.mean(samples),
        "p50": percentile(sorted_samples, 0.50),
        "p90": percentile(sorted_samples, 0.90),
        "p95": percentile(sorted_samples, 0.95),
        "max": max(samples),
    }


def format_summary(summary: Dict[str, float], scale: float) -> str:
    return (
        f"count={summary['count']:.0f} "
        f"avg={summary['avg'] / scale:.3f} "
        f"p50={summary['p50'] / scale:.3f} "
        f"p90={summary['p90'] / scale:.3f} "
        f"p95={summary['p95'] / scale:.3f} "
        f"max={summary['max'] / scale:.3f}"
    )


def analyze(log_path: Path, scale: float) -> None:
    per_message: Dict[str, Dict[str, List[float]]] = collections.defaultdict(
        lambda: collections.defaultdict(list)
    )

    sources_seen = set()

    with log_path.open() as fp:
        for line in fp:
            sample = parse_line(line)
            if not sample:
                continue

            msg = str(sample["message"])
            source = str(sample.get("source", "unknown"))
            message_key = (msg, source)
            sources_seen.add(source)
            for field in (
                "serialize_ns",
                "deserialize_ns",
                "send_ns",
                "recv_ns",
                "roundtrip_ns",
                "lineairdb_exec_ns",
            ):
                if field in sample:
                    per_message[message_key][field].append(sample[field])

    if not per_message:
        print("ログから有効なデータを読み込めませんでした。")
        return

    default_fields = [
        "serialize_ns",
        "deserialize_ns",
        "send_ns",
        "recv_ns",
        "roundtrip_ns",
        "lineairdb_exec_ns",
    ]

    fields = []
    for field in default_fields:
        if any(field in timings for timings in per_message.values()):
            fields.append(field)
    for timings in per_message.values():
        for field in timings.keys():
            if field not in fields and field != "message":
                fields.append(field)

    messages = sorted({message for (message, _) in per_message.keys()})

    def choose_samples(message: str, field: str) -> List[float]:
        source_priority = (
            ["server", "client", "unknown"]
            if field == "lineairdb_exec_ns"
            else ["client", "server", "unknown"]
        )
        for src in source_priority:
            samples = per_message.get((message, src), {}).get(field)
            if samples:
                return samples
        return []

    for message in messages:
        print(f"=== {message} ===")
        for field in fields:
            samples = choose_samples(message, field)
            summary = summarize(samples)
            print(f"{field:>15}: {format_summary(summary, scale)}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="protobuf_timing.log 解析ツール")
    parser.add_argument(
        "logfile",
        nargs="?",
        default="/home/noxy/ordo/lineairdb_logs/protobuf_timing.log",
        help="解析対象のログファイルパス",
    )
    parser.add_argument(
        "--unit",
        choices=("ns", "us", "ms"),
        default="us",
        help="表示単位 (ns/us/ms)",
    )
    args = parser.parse_args()

    scale_map = {"ns": 1.0, "us": 1_000.0, "ms": 1_000_000.0}
    scale = scale_map[args.unit]

    log_path = Path(args.logfile)
    if not log_path.exists():
        raise SystemExit(f"ログファイルが見つかりません: {log_path}")

    analyze(log_path, scale)


if __name__ == "__main__":
    main()
