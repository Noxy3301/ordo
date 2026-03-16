#!/usr/bin/env python3
"""
Ordo benchmark runner — YCSB and TPC-C via BenchBase.

Usage:
  # Single run (default: tpcc, 64 terminals, SF=1)
  python3 bench/bin/benchrun.py tpcc --terminals 64

  # Sweep thread counts
  python3 bench/bin/benchrun.py tpcc --sweep 1,4,16,32,64

  # YCSB with profile
  python3 bench/bin/benchrun.py ycsb --profile a --terminals 8 --scalefactor 100

Prerequisites:
  - Ordo server running (scripts/start_server.sh)
  - MySQL running with LineairDB plugin (scripts/start_mysql.sh)
  - BenchBase patched and built (bench/bin/patch_benchbase.py)
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCHBASE_DIR = ROOT / "third_party" / "benchbase" / "benchbase-mysql"
MYSQL_BIN = ROOT / "build" / "runtime_output_directory" / "mysql"

YCSB_PROFILES = {
    "a": "50,0,0,50,0,0",
    "b": "95,0,0,5,0,0",
    "c": "100,0,0,0,0,0",
    "e": "0,5,95,0,0,0",
    "f": "50,0,0,0,0,50",
}

os.environ["LD_PRELOAD"] = "/lib/x86_64-linux-gnu/libjemalloc.so.2"


def mysql_cmd(port, host, sql):
    """Execute SQL via mysql client."""
    cmd = [
        str(MYSQL_BIN), "-u", "root",
        "--protocol=TCP", "-h", host, "-P", str(port),
        "-e", sql,
    ]
    return subprocess.run(cmd, capture_output=True, text=True)


def update_xml(config_path, **kwargs):
    """Update XML config values using regex replacement."""
    text = config_path.read_text()
    for tag, value in kwargs.items():
        text = re.sub(rf"<{tag}>.*?</{tag}>", f"<{tag}>{value}</{tag}>", text)
    config_path.write_text(text)


def run_benchbase(benchmark, config_path, create=False, load=False, execute=False):
    """Run BenchBase with given phases."""
    jar = BENCHBASE_DIR / "benchbase.jar"
    if not jar.exists():
        print(f"ERROR: {jar} not found. Run: python3 bench/bin/patch_benchbase.py", file=sys.stderr)
        sys.exit(1)

    flags = f"--create={'true' if create else 'false'} --load={'true' if load else 'false'} --execute={'true' if execute else 'false'}"
    cmd = f"java -jar {jar} -b {benchmark} -c {config_path} {flags}"

    result = subprocess.run(
        cmd, shell=True, cwd=BENCHBASE_DIR,
        capture_output=True, text=True,
    )
    return result


def extract_throughput(output):
    """Parse throughput and goodput from BenchBase output."""
    match = re.search(
        r"Results\(.*?measuredRequests=(\d+)\)\s*=\s*([\d.]+)\s*requests/sec\s*\(throughput\),\s*([\d.]+)\s*requests/sec\s*\(goodput\)",
        output,
    )
    if match:
        return {
            "requests": int(match.group(1)),
            "throughput": float(match.group(2)),
            "goodput": float(match.group(3)),
        }
    return None


def extract_histograms(output):
    """Parse retry and error counts from BenchBase output."""
    info = {}
    for label, key in [
        ("Rejected Transactions (Server Retry)", "server_retry"),
        ("Unexpected SQL Errors", "unexpected_errors"),
    ]:
        match = re.search(rf"{re.escape(label)}.*?(?=\[0;1m|\Z)", output, re.DOTALL)
        if match:
            section = match.group(0)
            total = sum(int(n) for n in re.findall(r"\[\s*(\d+)\]", section))
            info[key] = total
    return info


def collect_results(result_dir):
    """Read summary.json from BenchBase results."""
    for f in sorted(result_dir.glob("*.summary.json"), reverse=True):
        with open(f) as fh:
            return json.load(fh)
    return None


def run_single(benchmark, config_path, terminals, mysql_host, mysql_port, result_base, **extra):
    """Run a single benchmark iteration: reset DB, create, load, execute."""
    db_name = "benchbase"

    print(f"\n{'='*50}")
    print(f"  {benchmark.upper()} | Terminals: {terminals}")
    print(f"{'='*50}")

    # Update terminals in config
    update_xml(config_path, terminals=str(terminals))

    # Reset database
    print("  Resetting database...")
    mysql_cmd(mysql_port, mysql_host, f"DROP DATABASE IF EXISTS {db_name}; CREATE DATABASE {db_name};")

    # TPC-H optimizer settings for hash join + subquery optimization
    if benchmark == "tpch":
        mysql_cmd(mysql_port, mysql_host,
                  "SET GLOBAL optimizer_switch='batched_key_access=on,mrr_cost_based=off,subquery_to_derived=on';"
                  "SET GLOBAL join_buffer_size=1073741824;")

    # Create + Load
    print("  Creating schema + Loading data...")
    result = run_benchbase(benchmark, config_path, create=True, load=True, execute=False)
    if result.returncode != 0:
        print(f"  ERROR during create/load:\n{result.stdout[-500:]}\n{result.stderr[-500:]}", file=sys.stderr)
        return None

    # Parse load time
    load_match = re.search(r"Finished executing.*?\[time=([\d.]+)s\]", result.stdout)
    load_time = float(load_match.group(1)) if load_match else None
    if load_time:
        print(f"  Load time: {load_time:.1f}s")

    # Execute
    print("  Executing benchmark (60s)...")
    result = run_benchbase(benchmark, config_path, create=False, load=False, execute=True)
    combined = result.stdout + result.stderr

    perf = extract_throughput(combined)
    histograms = extract_histograms(combined)

    if perf:
        print(f"  Throughput: {perf['throughput']:.1f} req/s | Goodput: {perf['goodput']:.1f} req/s")
        print(f"  Server Retry: {histograms.get('server_retry', 0)} | Unexpected Errors: {histograms.get('unexpected_errors', 0)}")
    else:
        print(f"  WARNING: Could not parse throughput from output")
        if result.returncode != 0:
            print(f"  BenchBase stderr (last 500 chars):\n{result.stderr[-500:]}")

    # Save results
    res_dir = result_base / f"thread_{terminals}"
    res_dir.mkdir(parents=True, exist_ok=True)

    bb_results = BENCHBASE_DIR / "results"
    if bb_results.exists():
        for csv in bb_results.glob("*.csv"):
            shutil.move(str(csv), str(res_dir / csv.name))
        for jf in bb_results.glob("*.json"):
            shutil.move(str(jf), str(res_dir / jf.name))

    return {
        "terminals": terminals,
        "load_time": load_time,
        **(perf or {}),
        **histograms,
    }


def main():
    parser = argparse.ArgumentParser(description="Ordo benchmark runner")
    parser.add_argument("benchmark", choices=["tpcc", "tpch", "ycsb"], help="Benchmark type")
    parser.add_argument("--terminals", type=int, default=64, help="Number of terminals (default: 64)")
    parser.add_argument("--sweep", type=str, help="Comma-separated thread counts to sweep (e.g. 1,4,16,64)")
    parser.add_argument("--scalefactor", type=float, default=1, help="Scale factor (default: 1)")
    parser.add_argument("--time", type=int, default=60, help="Execute time in seconds (default: 60)")
    parser.add_argument("--profile", type=str, default="a", help="YCSB profile: a,b,c,e,f (default: a)")
    parser.add_argument("--mysql-host", type=str, default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=3307)
    parser.add_argument("--loader-threads", type=int, default=1, help="Number of parallel loader threads (default: 1)")
    parser.add_argument("--restart-between", action="store_true", help="Restart Ordo server+MySQL between sweep iterations")
    parser.add_argument("--exclude-queries", type=str, help="TPC-H: comma-separated query numbers to exclude (e.g. 15,21)")
    args = parser.parse_args()

    # Validate
    jar = BENCHBASE_DIR / "benchbase.jar"
    if not jar.exists():
        print(f"ERROR: {jar} not found.\nRun: python3 bench/bin/patch_benchbase.py", file=sys.stderr)
        sys.exit(1)

    # Prepare config
    config_src = ROOT / "bench" / "config" / f"{args.benchmark}.xml"
    if not config_src.exists():
        print(f"ERROR: {config_src} not found", file=sys.stderr)
        sys.exit(1)

    # Work on a copy to avoid polluting the original
    config_dir = ROOT / "bench" / "config" / "generated"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_work = config_dir / f"{args.benchmark}.run.xml"
    shutil.copy2(config_src, config_work)

    # Update config
    update_xml(config_work, scalefactor=str(args.scalefactor), time=str(args.time))
    # TPC-H with multiple terminals needs parallel mode (serial=false + time tag)
    if args.benchmark == "tpch" and (args.sweep or args.terminals > 1):
        text = config_work.read_text()
        text = text.replace("<serial>true</serial>", "<serial>false</serial>")
        if "<time>" not in text:
            text = text.replace(
                "<serial>false</serial>",
                f"<serial>false</serial>\n            <time>{args.time}</time>",
            )
        config_work.write_text(text)
    if args.loader_threads > 1:
        text = config_work.read_text()
        if "<loaderThreads>" in text:
            update_xml(config_work, loaderThreads=str(args.loader_threads))
        else:
            text = text.replace("</parameters>", f"    <loaderThreads>{args.loader_threads}</loaderThreads>\n</parameters>")
            config_work.write_text(text)
    if args.benchmark == "tpch" and args.exclude_queries:
        exclude_set = {int(q.strip()) for q in args.exclude_queries.split(",")}
        text = config_work.read_text()
        # TPC-H has 22 queries, weights is "1,1,...,1" (22 values)
        import re
        match = re.search(r"<weights>([\d,]+)</weights>", text)
        if match:
            weights = match.group(1).split(",")
            for q in exclude_set:
                if 1 <= q <= len(weights):
                    weights[q - 1] = "0"
            new_weights = ",".join(weights)
            text = text.replace(match.group(0), f"<weights>{new_weights}</weights>", 1)
            config_work.write_text(text)
            print(f"  Excluded TPC-H queries: {sorted(exclude_set)}")
    if args.benchmark == "ycsb":
        weights = YCSB_PROFILES.get(args.profile)
        if not weights:
            print(f"ERROR: Unknown YCSB profile '{args.profile}'. Options: {list(YCSB_PROFILES.keys())}", file=sys.stderr)
            sys.exit(1)
        update_xml(config_work, weights=weights)

    # Determine thread list
    if args.sweep:
        thread_list = [int(t.strip()) for t in args.sweep.split(",")]
    else:
        thread_list = [args.terminals]

    # Result directory
    now = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    result_base = ROOT / "bench" / "results" / now / args.benchmark.upper()
    result_base.mkdir(parents=True, exist_ok=True)

    print(f"Benchmark: {args.benchmark.upper()}")
    print(f"Threads:   {thread_list}")
    print(f"SF={args.scalefactor}, Time={args.time}s, MySQL={args.mysql_host}:{args.mysql_port}")
    print(f"Results:   {result_base}")

    # Run
    all_results = []
    for terminals in thread_list:
        if args.restart_between and len(thread_list) > 1:
            print("\n  Restarting Ordo server + MySQL...")
            subprocess.run([str(ROOT / "scripts" / "stop_server.sh")])
            subprocess.run([str(ROOT / "scripts" / "stop_mysql.sh")])
            time.sleep(5)
            subprocess.run([str(ROOT / "scripts" / "start_server.sh")])
            time.sleep(3)
            subprocess.run([
                str(ROOT / "scripts" / "start_mysql.sh"),
                "--mysqld-port", str(args.mysql_port),
                "--server-host", "127.0.0.1", "--server-port", "9999",
            ])
            time.sleep(5)

        result = run_single(
            args.benchmark, config_work, terminals,
            args.mysql_host, args.mysql_port, result_base,
        )
        if result:
            all_results.append(result)

    # Summary
    print(f"\n{'='*60}")
    print(f"  SUMMARY: {args.benchmark.upper()} SF={args.scalefactor}")
    print(f"{'='*60}")
    print(f"{'Threads':>8} {'Throughput':>12} {'Goodput':>10} {'Retry':>8} {'Errors':>8}")
    print(f"{'-'*8:>8} {'-'*12:>12} {'-'*10:>10} {'-'*8:>8} {'-'*8:>8}")
    for r in all_results:
        print(f"{r['terminals']:>8} {r.get('throughput', 0):>12.1f} {r.get('goodput', 0):>10.1f} {r.get('server_retry', 0):>8} {r.get('unexpected_errors', 0):>8}")

    # Save summary CSV
    csv_path = result_base / "summary.csv"
    with open(csv_path, "w") as f:
        f.write("terminals,throughput,goodput,server_retry,unexpected_errors,load_time\n")
        for r in all_results:
            f.write(f"{r['terminals']},{r.get('throughput',0):.1f},{r.get('goodput',0):.1f},{r.get('server_retry',0)},{r.get('unexpected_errors',0)},{r.get('load_time','')}\n")
    print(f"\nResults saved: {csv_path}")


if __name__ == "__main__":
    main()
