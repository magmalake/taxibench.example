#!/usr/bin/env python3
"""Diff two result files and report the timings side by side.

    compare.py <mojo.jsonl> <python.jsonl> [--json out.json]

The comparison is the point. Two implementations that disagree about how many
cash trips there were in 2024 are not two implementations of the same query,
and whichever is faster is then irrelevant. Counts must match exactly; sums are
allowed a relative tolerance, because the two sum the same doubles in a
different order and the last bits legitimately differ.

Exit status is 1 if any answer disagrees, so this works as a CI gate.
"""

from __future__ import annotations

import argparse
import json
import sys

# Floating-point sums over ~10^8 values accumulate in a different order on each
# side. The worst case here is a sum near 3e9 over 78M doubles, where the
# orders can drift by a few parts in 10^12 without anything being wrong.
REL_TOLERANCE = 1e-9


def load(path: str) -> dict[str, dict]:
    records = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                record = json.loads(line)
                records[record["query"]] = record
    return records


def compare_values(name: str, a, b, path: str = "") -> list[str]:
    """Structural comparison; returns a list of human-readable differences."""
    where = f"{name}{path}"
    if isinstance(a, bool) or isinstance(b, bool):
        return [] if a == b else [f"{where}: {a} != {b}"]
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, int) and isinstance(b, int):
            return [] if a == b else [f"{where}: {a} != {b}"]
        scale = max(abs(a), abs(b), 1.0)
        if abs(a - b) / scale <= REL_TOLERANCE:
            return []
        return [f"{where}: {a!r} != {b!r} (rel {abs(a - b) / scale:.2e})"]
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return [f"{where}: length {len(a)} != {len(b)}"]
        out = []
        for i, (x, y) in enumerate(zip(a, b)):
            out += compare_values(name, x, y, f"{path}[{i}]")
        return out
    if isinstance(a, dict) and isinstance(b, dict):
        if a.keys() != b.keys():
            return [f"{where}: keys {sorted(a)} != {sorted(b)}"]
        out = []
        for key in a:
            out += compare_values(name, a[key], b[key], f"{path}.{key}")
        return out
    return [] if a == b else [f"{where}: {a!r} != {b!r}"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mojo")
    ap.add_argument("python")
    ap.add_argument("--json", help="also write the merged table here")
    ap.add_argument("--label", default="", help="thread configuration of this leg")
    args = ap.parse_args()

    mojo, python = load(args.mojo), load(args.python)
    names = [n for n in mojo if n in python]
    only_mojo = sorted(set(mojo) - set(python))
    only_python = sorted(set(python) - set(mojo))

    problems: list[str] = []
    for name in only_mojo:
        problems.append(f"{name}: only in {args.mojo}")
    for name in only_python:
        problems.append(f"{name}: only in {args.python}")

    header = (
        f"{'query':<18}{'files':>6}{'rows':>12}"
        f"{'mojo p50':>10}{'p90':>8}"
        f"{'pyice p50':>11}{'p90':>8}{'ratio':>8}  answers"
    )
    if args.label:
        print(f"({args.label})")
    print(header)
    print("-" * len(header))

    merged = []
    for name in names:
        m, p = mojo[name], python[name]
        differences = compare_values(name, m["result"], p["result"])
        if m["rows_scanned"] != p["rows_scanned"]:
            differences.append(
                f"{name}.rows_scanned: {m['rows_scanned']} != {p['rows_scanned']}"
            )
        if m["files"] != p["files"]:
            differences.append(f"{name}.files: {m['files']} != {p['files']}")
        problems += differences

        speedup = p["total_ms"] / m["total_ms"] if m["total_ms"] else 0.0
        print(
            f"{name:<18}{m['files']:>6}{m['rows_scanned']:>12,}"
            f"{m['total_ms']:>10.1f}{m.get('p90_ms', 0):>8.1f}"
            f"{p['total_ms']:>11.1f}{p.get('p90_ms', 0):>8.1f}"
            f"{speedup:>7.2f}x  {'match' if not differences else 'MISMATCH'}"
        )
        merged.append(
            {
                "query": name,
                "title": m["title"],
                "files": m["files"],
                "rows_scanned": m["rows_scanned"],
                "mojo_ms": m["total_ms"],
                "mojo_p90_ms": m.get("p90_ms"),
                "mojo_min_ms": m.get("min_ms"),
                "mojo_plan_ms": m["plan_ms"],
                "mojo_workers": m.get("workers", 1),
                "python_ms": p["total_ms"],
                "python_p90_ms": p.get("p90_ms"),
                "python_min_ms": p.get("min_ms"),
                "python_plan_ms": p["plan_ms"],
                "python_threads": p.get("threads"),
                "leg": args.label,
                "speedup": round(speedup, 3),
                "answers_match": not differences,
            }
        )

    if merged:
        mojo_total = sum(r["mojo_ms"] for r in merged)
        python_total = sum(r["python_ms"] for r in merged)
        print("-" * len(header))
        print(
            f"{'total':<18}{'':>6}{'':>12}{mojo_total:>10.1f}{'':>8}"
            f"{python_total:>11.1f}{'':>8}{python_total / mojo_total:>7.2f}x"
        )

    if args.json:
        with open(args.json, "w") as fh:
            json.dump(merged, fh, indent=2)

    if problems:
        print("\nanswers disagree:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    print("\nall answers agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
