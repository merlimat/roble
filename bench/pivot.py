# Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Turns the engines' `--tsv` lines into one table: a row per operation and
metric, a column per engine.

Reads the lines on stdin, in any order, and prints the engines in the order
they first appear so the subject is wherever `run.sh` put it. A phase an
engine skipped shows the reason once, under the table, rather than a blank
that reads as a zero.

Usage: pivot.py [--metrics rate,p50,cpu,size] < lines
"""

import sys

# The phases, in the order they run, so the table reads like the run did.
PHASES = [
    "load",
    "settle",
    "scan-rev",
    "scan",
    "get",
    "floor",
    "get-cold",
    "checkpoint",
]

# What each metric is called, which column of the line holds it, and how to
# spell a value. `rate` first: it is the headline for every phase.
def _num(value, fmt):
    """Formats a measurement, or passes a dash straight through.

    A phase can decline a column — `load` reports no p50 or p99, because its
    latency is per commit where its count and rate are per record — and says
    so with a dash. Formatting it as a number would raise; formatting the
    dash as a number is what a reader wants to see anyway.
    """
    try:
        return fmt(value)
    except ValueError:
        return value


METRICS = {
    "rate": ("rate/s", 3, lambda v: _num(v, lambda x: f"{float(x):,.0f}")),
    "p50": ("p50 us", 4, lambda v: _num(v, lambda x: f"{int(x):,}")),
    "p99": ("p99 us", 5, lambda v: _num(v, lambda x: f"{int(x):,}")),
    "max": ("max us", 6, lambda v: _num(v, lambda x: f"{int(x):,}")),
    "cpu": ("cpu s", 7, lambda v: _num(v, lambda x: f"{float(x):.2f}")),
    "size": ("size MiB", 8, lambda v: _num(v, lambda x: f"{float(x):.1f}")),
}


def main() -> int:
    args = sys.argv[1:]
    chosen = ["rate", "p50", "cpu"]
    if "--metrics" in args:
        chosen = args[args.index("--metrics") + 1].split(",")
    for m in chosen:
        if m not in METRICS:
            print(f"pivot: unknown metric {m}", file=sys.stderr)
            return 2

    engines: list[str] = []
    cells: dict[tuple[str, str, str], str] = {}
    skipped: list[tuple[str, str, str]] = []
    rss: dict[str, str] = {}

    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 3:
            continue
        engine, phase = parts[0], parts[1]
        if engine not in engines:
            engines.append(engine)
        if phase == "peak-rss-mib":
            rss[engine] = parts[2]
            continue
        if parts[2] == "skipped":
            skipped.append((engine, phase, parts[3] if len(parts) > 3 else ""))
            continue
        if len(parts) < 9:
            continue
        for metric in chosen:
            _, at, spell = METRICS[metric]
            cells[(phase, metric, engine)] = spell(parts[at])

    if not engines:
        print("pivot: nothing on stdin", file=sys.stderr)
        return 1

    width = max(12, max(len(e) for e in engines) + 2)
    print(f"{'':<10} {'':<9}" + "".join(f"{e:>{width}}" for e in engines))
    for phase in PHASES:
        rows = [m for m in chosen if any((phase, m, e) in cells for e in engines)]
        if not rows:
            # Skipped by every engine that ran, or never asked for.
            continue
        for i, metric in enumerate(rows):
            label, _, _ = METRICS[metric]
            name = phase if i == 0 else ""
            line = f"{name:<10} {label:<9}"
            for engine in engines:
                line += f"{cells.get((phase, metric, engine), '—'):>{width}}"
            print(line)
    if rss:
        line = f"{'peak rss':<10} {'MiB':<9}"
        for engine in engines:
            line += f"{rss.get(engine, '—'):>{width}}"
        print(line)

    for engine, phase, why in skipped:
        print(f"\n{engine} skipped {phase}: {why}", end="")
    if skipped:
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
