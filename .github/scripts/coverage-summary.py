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

"""Summarizes a `cargo llvm-cov --json` report in Markdown: coverage by file, then
each line no test runs and, on lines that do run, each region that doesn't, such
as the error branch of a `?`.

Usage: coverage-summary.py REPORT [--fail-under PERCENT]

In GitHub Actions, it also writes the summary to the job summary, and sets the
step output `totals` to the totals on one line, as in
`lines 97.8% (65 missed), regions 97.9% (117 missed), functions 97.8% (8 missed)`.

`--fail-under PERCENT` prints nothing but a verdict and exits non-zero if line
coverage is below PERCENT. The percentage compared is the one this script
prints, floored to a tenth, so the rule is exactly "the number CI posted last
time must not go down" — no epsilon to reason about, and a run that rounds to
the same tenth passes.

Line coverage is counted here, by the same walk that finds the lines to list,
rather than taken from llvm-cov's own summary. That summary adds up its lines
function by function, and a function compiled more than once — into the unit
tests and into the library the integration tests link, and a generic one again
for each type each test crate uses it with — is credited with whichever single
copy ran the most of it. A line that only another copy ran then counts as
missed, yet appears nowhere in the listing, which merges every copy, so
nobody could find it to cover it; and moving a test from one file to another
could move the number. Counted from the listing, every line the percentage
misses is a line the listing shows.
"""

import json
import math
import os
import sys

KINDS = ("lines", "regions", "functions")


def main():
    args = sys.argv[1:]
    floor = None
    if "--fail-under" in args:
        at = args.index("--fail-under")
        floor = float(args[at + 1])
        del args[at : at + 2]
    with open(args[0]) as f:
        report = json.load(f)["data"][0]
    count_lines(report)
    if floor is not None:
        sys.exit(verdict(report, floor))
    text = "\n".join(summary(report)) + "\n"
    print(text, end="")
    if "GITHUB_STEP_SUMMARY" in os.environ:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as out:
            out.write(text)
    if "GITHUB_OUTPUT" in os.environ:
        totals = report["totals"]
        with open(os.environ["GITHUB_OUTPUT"], "a") as out:
            line = ", ".join(f"{kind} {coverage(totals[kind])}" for kind in KINDS)
            print(f"totals={line}", file=out)


def verdict(report, floor):
    """Prints whether line coverage held at `floor`, and returns an exit status.

    Only lines are gated. Regions and functions move for reasons lines do not
    — a refactor that splits one expression into two changes the region count
    without changing what any test reaches — and three gates would fail three
    times as often for reasons nobody acted on.
    """
    now = tenth(report["totals"]["lines"])
    if now < floor:
        print(
            f"Coverage dropped: lines {now:.1f}%, below the {floor:.1f}% on this "
            f"branch's base.\n"
            f"Cover what the change added, or say in the pull request why the "
            f"line that is now unreached should stay that way (DESIGN.md §12)."
        )
        return 1
    print(f"Coverage held: lines {now:.1f}%, against {floor:.1f}% on the base.")
    return 0


def count_lines(report):
    """Replaces llvm-cov's line counts, each file's and the total, with the
    lines `misses` finds: every line that maps to code, and of those, every
    line some test ran."""
    total = {"count": 0, "covered": 0}
    for file in report["files"]:
        mapped, unrun, _ = misses(file["segments"])
        lines = {"count": len(mapped), "covered": len(mapped) - len(unrun)}
        file["summary"]["lines"] = with_percent(lines)
        total["count"] += lines["count"]
        total["covered"] += lines["covered"]
    report["totals"]["lines"] = with_percent(total)


def with_percent(summary):
    summary["percent"] = 100 * summary["covered"] / summary["count"]
    return summary


def tenth(summary):
    """The percentage as this script prints it: floored to a tenth, so that
    only full coverage shows as 100%."""
    return math.floor(summary["percent"] * 10) / 10


def summary(report):
    """The summary's lines."""
    files = sorted(report["files"], key=lambda f: f["filename"])
    yield "## Coverage"
    yield ""
    yield "| File | Lines | Regions | Functions |"
    yield "|---|---|---|---|"
    rows = [(f"`{path(f)}`", f["summary"]) for f in files]
    for name, s in rows + [("**Total**", report["totals"])]:
        yield f"| {name} | {' | '.join(coverage(s[kind]) for kind in KINDS)} |"

    yield ""
    yield "### Not covered"
    yield ""
    yield (
        "Each line no test runs, and on lines that do run, a `^` under each region "
        "that doesn't."
    )
    for file in files:
        _, unrun, gaps = misses(file["segments"])
        if not unrun and not gaps:
            continue
        with open(file["filename"], encoding="utf-8") as f:
            source = f.read().split("\n")
        yield ""
        yield f"`{path(file)}`"
        yield ""
        yield "```text"
        previous = None
        for line in sorted(unrun | gaps.keys()):
            if previous is not None and line > previous + 1:
                yield "  ..."
            text = source[line - 1]
            yield f"{line:5} | {text}"
            if line in gaps:
                yield f"{'':5} | {markers(text, gaps[line])}"
            previous = line
        yield "```"


def path(file):
    return os.path.relpath(file["filename"])


def coverage(summary):
    """As in `97.8% (65 missed)`, rounded down so that only full coverage shows as
    100%."""
    percent = tenth(summary)
    missed = summary["count"] - summary["covered"]
    return f"{percent:.1f}%" + (f" ({missed} missed)" if missed else "")


def misses(segments):
    """Returns the lines that map to code, those of them that never ran, and for
    each line that did, the columns where regions that never ran start. Lines
    count as llvm-cov counts them line by line, every copy of the code merged.

    A segment is [line, column, count, has count, starts a region, is a gap].
    """
    mapped_lines, unrun, gaps = set(), set(), {}
    wrapped = None  # The last segment before the line.
    at = 0
    for line in range(1, segments[-1][0] + 1 if segments else 1):
        here = []
        while at < len(segments) and segments[at][0] == line:
            here.append(segments[at])
            at += 1
        starts = [s for s in here if s[3] and s[4] and not s[5]]
        skipped = here and not here[0][3] and here[0][4]
        mapped = not skipped and (wrapped and wrapped[3] or starts)
        if mapped or any(s[3] and s[4] for s in here):
            mapped_lines.add(line)
            count = max([wrapped[2] if wrapped else 0] + [s[2] for s in starts])
            if count == 0:
                unrun.add(line)
            elif cols := [s[1] for s in starts if s[2] == 0]:
                gaps[line] = cols
        if here:
            wrapped = here[-1]
    return mapped_lines, unrun, gaps


def markers(text, cols):
    """A `^` under each of the 1-based byte columns `cols` of `text`."""
    raw = text.encode()
    at = {len(raw[: col - 1].decode(errors="replace")) for col in cols}
    return "".join("^" if i in at else " " for i in range(max(at) + 1))


if __name__ == "__main__":
    main()
