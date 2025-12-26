#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROLE_MAP = [
    {"tag": "ordo-lineairdb", "group": "lineairdb", "prefix": "lineairdb"},
    {"tag": "ordo-mysql", "group": "mysql", "prefix": "mysql"},
    {"tag": "ordo-haproxy", "group": "haproxy", "prefix": "haproxy"},
    {"tag": "ordo-bench", "group": "benchbase", "prefix": "bench"},
]


def run_aws(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr.strip() or "aws command failed", file=sys.stderr)
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"failed to parse aws output: {exc}", file=sys.stderr)
        return None


def build_inventory(instances, args):
    by_tag = {}
    for inst in instances:
        name = inst.get("Name")
        if not name:
            continue
        by_tag.setdefault(name, []).append(inst)

    lines = []
    lines.append("[all:vars]")
    lines.append(f"ansible_user={args.user}")
    lines.append(f"ansible_ssh_private_key_file={args.key}")

    for role in ROLE_MAP:
        group = role["group"]
        prefix = role["prefix"]
        tag = role["tag"]
        entries = by_tag.get(tag, [])
        entries.sort(key=lambda item: item.get("PrivIP") or "")
        lines.append("")
        lines.append(f"[{group}]")
        for idx, inst in enumerate(entries, start=1):
            private_ip = inst.get("PrivIP")
            public_ip = inst.get("PubIP")
            if args.ansible_host == "private":
                ansible_host = private_ip or public_ip
            else:
                ansible_host = public_ip or private_ip
            if not ansible_host:
                print(f"skip {tag} missing IP: {inst}", file=sys.stderr)
                continue
            host = f"{prefix}-{idx}"
            lines.append(f"{host} ansible_host={ansible_host} private_ip={private_ip}")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate ansible/inventory.ini from AWS tags")
    parser.add_argument("--region", type=str, default="ap-southeast-2", help="AWS region")
    parser.add_argument("--profile", type=str, default=None, help="AWS profile (optional)")
    parser.add_argument("--project-tag", type=str, default="Ordo", help="Project tag value")
    parser.add_argument("--user", type=str, default="ubuntu", help="ansible_user")
    parser.add_argument("--key", type=str, default="~/.ssh/ordo-aws.pem", help="ssh private key path")
    parser.add_argument(
        "--ansible-host",
        choices=["public", "private"],
        default="public",
        help="which IP to set for ansible_host",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "inventory.ini",
        help="output inventory path",
    )
    args = parser.parse_args()

    tags = [role["tag"] for role in ROLE_MAP]
    filters = ["Name=instance-state-name,Values=running"]
    if args.project_tag:
        filters.append(f"Name=tag:Project,Values={args.project_tag}")
    filters.append("Name=tag:Name,Values=" + ",".join(tags))

    cmd = [
        "aws",
        "ec2",
        "describe-instances",
        "--filters",
        *filters,
        "--query",
        "Reservations[].Instances[].{Name:Tags[?Key=='Name']|[0].Value,PrivIP:PrivateIpAddress,PubIP:PublicIpAddress}",
        "--output",
        "json",
    ]
    if args.region:
        cmd.extend(["--region", args.region])
    if args.profile:
        cmd.extend(["--profile", args.profile])

    data = run_aws(cmd)
    if data is None:
        return 1

    inventory = build_inventory(data, args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(inventory)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
