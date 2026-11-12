#!/usr/bin/env python3
"""Read-only, windowed host-pin audit through the installed-authority API client."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients/python/src"))
from mycelium import APIError, Client, UnsupportedTLSRuntimeError


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def audit(client: Client, since: datetime, until: datetime) -> tuple[dict[str, Any], dict[str, str]]:
    """Count explicit authored pins, once per returned job, in [since, until)."""
    if since >= until:
        raise ValueError("since must precede until")
    jobs: dict[str, dict[str, Any]] = {}
    before = None
    cursors: set[str] = set()
    while True:
        page = client.status(jobs="all", since=since.isoformat(), limit=1000, before=before)
        if "jobs_page" not in page:
            raise ValueError("queue must support paginated status; upgrade before auditing")
        for job in page.get("jobs") or []:
            if since <= timestamp(job["queued"]) < until:
                jobs[job["id"]] = job
        before = page["jobs_page"].get("next_before")
        if not before:
            break
        if before in cursors:
            raise ValueError("status pagination did not advance")
        cursors.add(before)

    projects = sorted({j["spec"]["project"] for j in jobs.values()})
    labels = {project: f"project-{i + 1}" for i, project in enumerate(projects)}
    groups: dict[tuple[str, str], list[int]] = {}
    for job in jobs.values():
        spec = job["spec"]
        omitted = (job.get("spec_projection") or {}).get("omitted", [])
        if "allowed_machines" in omitted:
            raise ValueError("queue projection omits authored host pins; audit is unknown")
        # Device/target restrictions are scheduler state
        # or software requirements, not authored host pins. Never count them.
        pinned = bool(spec.get("allowed_machines")) or (job.get("placement_intent") or {}).get("host_constraint") == "pin"
        category = "production" if (spec.get("class") or spec.get("priority")) == "production" else "non-production"
        for key in [("all", "all"), ("all", category), (labels[spec["project"]], "all"), (labels[spec["project"]], category)]:
            counts = groups.setdefault(key, [0, 0])
            counts[0] += int(pinned)
            counts[1] += 1
    rows = [{"project": project, "class": category, "pinned": counts[0], "jobs": counts[1],
             "pinned_percent": round(100 * counts[0] / counts[1], 3)}
            for (project, category), counts in sorted(groups.items())]
    report = {
        "since": since.isoformat(), "until_exclusive": until.isoformat(),
        "method": "2026-10-30: explicit spec.allowed_machines or authored host_constraint=pin; jobs grouped by project and production/non-production",
        "coverage": "Retained API job records by queued timestamp; collection may remove old records. Pages are read-only observations, not a transaction across pages. No inferred pins from targets, device selectors or policy.",
        "rows": rows,
    }
    return report, {label: project for project, label in labels.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, type=timestamp, help="inclusive RFC3339 start")
    parser.add_argument("--until", type=timestamp, default=datetime.now(timezone.utc), help="exclusive RFC3339 end (default: now)")
    parser.add_argument("--private-project-map", type=Path, help="optional NEW private file mapping generic labels to names; never publish it")
    args = parser.parse_args()
    try:
        report, names = audit(Client.from_installed(), args.since, args.until)
        if args.private_project_map is not None:
            fd = os.open(args.private_project_map, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as output:
                json.dump(names, output, indent=2)
                output.write("\n")
    except (ValueError, OSError, APIError, UnsupportedTLSRuntimeError) as error:
        # A connection error can carry the queue address or private config path.
        print(f"audit failed ({type(error).__name__}); no complete measurement", file=sys.stderr)
        raise SystemExit(1) from None
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
