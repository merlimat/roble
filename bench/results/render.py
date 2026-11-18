#!/usr/bin/env python3
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

"""Turns the benchmark results kept here into charts and tables.

Each dated folder holds the CSVs one session measured. This draws an SVG
chart beside each, and writes README.md with the charts and every number as
a table. Python's standard library only, so it runs anywhere:

    python3 bench/results/render.py

A chart's rows are phases, and an engine's bar in a row is its rate against
the fastest engine in that row: rates differ by orders of magnitude between
phases (a checkpoint against a scan), so one scale for all of them would
leave most bars invisible. The rate itself is written beside each bar."""

import csv
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent

STANDARD = HERE / "2026-11-18-standard-1369bad"
PEBBLE = HERE / "2026-11-16-pebble-71cd740"
SINGLE = HERE / "2026-11-13-single-shard"

NAMES = {"roble": "roble", "rocksdb": "RocksDB", "encina": "encina", "pebble": "Pebble"}
COLORS = {"roble": "#41704A", "RocksDB": "#6B7C93", "encina": "#C98A2E", "Pebble": "#8A6BB0"}

PHASES = ["load", "scan", "scan-rev", "get", "floor", "scan-short", "checkpoint"]
YCSB = ["ycsb-A", "ycsb-B", "ycsb-C", "ycsb-D", "ycsb-E", "ycsb-F"]
SCENARIOS = {"in-cache": "In cache", "beyond-memory": "Past memory"}


def rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def num(s):
    return float(s) if s not in (None, "") else None


def human(v):
    if v is None:
        return "—"
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}k"
    return f"{v:.0f}"


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;")


def chart(path, title, subtitle, engines, data):
    """Grouped horizontal bars: `data` is [(row label, {engine: rate})]."""
    bar, gap, left, width, value_w = 11, 12, 118, 560, 64
    top = 74
    group = bar * len(engines) + gap
    height = top + group * len(data) + 18
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {left + width + value_w + 16} {height}" '
           'font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif">',
           f'<rect width="100%" height="100%" rx="10" fill="#ffffff"/>',
           f'<text x="16" y="26" font-size="16" font-weight="600" fill="#1f2328">{esc(title)}</text>',
           f'<text x="16" y="45" font-size="11.5" fill="#57606a">{esc(subtitle)}</text>']
    x = left
    for e in engines:
        out.append(f'<rect x="{x}" y="55" width="10" height="10" rx="2" fill="{COLORS[e]}"/>'
                   f'<text x="{x + 14}" y="64" font-size="11.5" fill="#1f2328">{e}</text>')
        x += 14 + 8 * len(e) + 18
    y = top
    for label, values in data:
        best = max((v for v in values.values() if v), default=None)
        out.append(f'<text x="{left - 10}" y="{y + group / 2 - 2:.1f}" font-size="12" '
                   f'text-anchor="end" fill="#1f2328">{esc(label)}</text>')
        for i, e in enumerate(engines):
            v = values.get(e)
            by = y + i * bar
            if v:
                w = max(1.5, width * v / best)
                out.append(f'<rect x="{left}" y="{by}" width="{w:.1f}" height="{bar - 2}" rx="2" fill="{COLORS[e]}"/>')
                out.append(f'<text x="{left + w + 5:.1f}" y="{by + bar - 3}" font-size="9.5" fill="#57606a">{human(v)}</text>')
            else:
                out.append(f'<text x="{left + 4}" y="{by + bar - 3}" font-size="9.5" fill="#8c959f">—</text>')
        y += group
    out.append("</svg>\n")
    path.write_text("\n".join(out))


def table(header, body):
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join(["---"] + ["---:"] * (len(header) - 1)) + "|"]
    lines += ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join(lines)


def standard(md):
    data = rows(STANDARD / "standard.csv")
    engines = ["roble", "RocksDB", "encina"]
    by = {(r["scenario"], r["phase"], NAMES[r["engine"]]): r for r in data}
    md.append("## The standard run\n")
    md.append("`bench/run.sh` on a Linux VM at main `1369bad`, 18 November 2026: every engine held to "
              "one CPU and 512 MiB (page cache included), the WAL off as Oxia runs it, over a store that "
              "fits the 256 MiB block cache (1,400,000 records) and one that does not fit in memory at all "
              "(6,000,000). Each cell is the median of three runs, taken with the VM's file indexer off. "
              "encina has no reverse scan and no checkpoint. Data: "
              f"[`standard.csv`]({STANDARD.name}/standard.csv).\n")
    for scenario, name in SCENARIOS.items():
        for kind, phases, unit in (("phases", PHASES, "records, keys or lookups a second"),
                                   ("ycsb", YCSB, "YCSB operations a second")):
            svg = STANDARD / f"{scenario}-{kind}.svg"
            chart(svg, f"{name}: {'every phase' if kind == 'phases' else 'YCSB A to F'}",
                  f"{unit}; each row scaled to its fastest engine", engines,
                  [(p, {e: num(by.get((scenario, p, e), {}).get("rate")) for e in engines}) for p in phases])
            md.append(f"![{name}, {kind}]({STANDARD.name}/{svg.name})\n")
        body = []
        for p in PHASES + YCSB:
            cells = []
            for e in engines:
                r = by.get((scenario, p, e), {})
                rate, p50 = num(r.get("rate")), r.get("p50_us")
                cells.append(f"{human(rate)}" + (f" · p50 {p50} µs" if rate and p50 else ""))
            body.append([p] + cells)
        rss = [by.get((scenario, "peak rss", e), {}).get("peak_rss_mib") or "—" for e in engines]
        body.append(["peak RSS, MiB"] + rss)
        md.append(f"<details><summary>{name}: every number</summary>\n\n"
                  + table(["phase"] + engines, body) + "\n\n</details>\n")


def pebble(md):
    data = rows(PEBBLE / "pebble-ycsb.csv")
    engines = ["roble", "RocksDB", "Pebble"]
    rates = {}
    for r in data:
        rates.setdefault((r["scenario"], r["phase"], NAMES[r["engine"]]), []).append(float(r["rate"]))
    phases = sorted({r["phase"] for r in data})
    md.append("## Go Pebble beside them\n")
    md.append("YCSB A, B, C and E on roble, RocksDB and Go Pebble v2.1.7 in one session on the same VM, "
              "16 November 2026, roble at `71cd740`, each under the same one CPU and 512 MiB, Pebble with "
              "`GOMAXPROCS=1` and the settings RocksDB gets here. Each bar is the median of three rounds. "
              "The Pebble driver that took these is not in this repository. Data: "
              f"[`pebble-ycsb.csv`]({PEBBLE.name}/pebble-ycsb.csv).\n")
    rows_ = []
    for scenario, name in SCENARIOS.items():
        for p in phases:
            rows_.append((f"{name} · {p.replace('ycsb-', '')}",
                          {e: statistics.median(rates[(scenario, p, e)]) if (scenario, p, e) in rates else None
                           for e in engines}))
    svg = PEBBLE / "pebble-ycsb.svg"
    chart(svg, "YCSB beside Go Pebble", "operations a second, median of three rounds; each row scaled to its fastest",
          engines, rows_)
    md.append(f"![YCSB beside Go Pebble]({PEBBLE.name}/{svg.name})\n")
    body = []
    for label, values in rows_:
        cells = []
        scenario = "in-cache" if label.startswith("In cache") else "beyond-memory"
        p = "ycsb-" + label.split("· ")[1]
        for e in engines:
            got = rates.get((scenario, p, e))
            cells.append(f"{human(statistics.median(got))} ({human(min(got))}–{human(max(got))})" if got else "—")
        body.append([label] + cells)
    md.append("<details><summary>Every number: median (slowest–fastest round)</summary>\n\n"
              + table(["mix"] + engines, body) + "\n\n</details>\n")


def single_shard(md):
    md.append("## An earlier single-shard run\n")
    md.append("13 November 2026, before the standard above existed: one shard, a 42 MiB store, all four "
              "engines on the same Linux machine in one run. Kept for what it "
              "measured that the standard does not: the WAL on and off, one record a commit, the cache in "
              "and out, and where a scan's time went. Its numbers are not comparable with the sections "
              "above.\n")
    engines = ["roble", "Pebble", "RocksDB", "encina"]
    data = rows(SINGLE / "phases.csv")
    by = {(r["phase"], r["engine"]): r for r in data}
    phases = [p for p in dict.fromkeys(r["phase"] for r in data) if p != "settle"]
    svg = SINGLE / "phases.svg"
    chart(svg, "Single shard: every phase", "records, keys or lookups a second; each row scaled to its fastest",
          engines, [(p, {e: num(by.get((p, e), {}).get("rate")) for e in engines}) for p in phases])
    md.append(f"![Single shard, every phase]({SINGLE.name}/{svg.name})\n")
    for name, title in (("phases", "Phase rates"), ("writes", "Writes: journal on and off, batched and one a commit"),
                        ("cache", "Reads with the cache holding the store, and an eighth of it"),
                        ("layout", "Tables per level after settling"),
                        ("scanwork", "roble's scan rate at each step of that week's work"),
                        ("steps", "A scan step's fixed and per-byte cost")):
        data = rows(SINGLE / f"{name}.csv")
        header = list(data[0].keys())
        md.append(f"<details><summary>{title} (<code>{name}.csv</code>)</summary>\n\n"
                  + table(header, [[r[h] for h in header] for r in data]) + "\n\n</details>\n")


def main():
    md = ["<!-- Generated by render.py from the CSVs beside it: edit those, or the script, and run it again. -->\n",
          "# Benchmark results\n",
          "What `roble-bench` measured, kept with the data it was drawn from. How the runs are made and "
          "what each phase does is in [the bench README](../README.md). A rate here is a property of one "
          "machine on one day: read the engines against each other within a run, not a rate against "
          "another run.\n"]
    standard(md)
    pebble(md)
    single_shard(md)
    (HERE / "README.md").write_text("\n".join(md))


if __name__ == "__main__":
    main()
