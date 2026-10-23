"""Propose an exception baseline from complete security evidence for owner review.

Existing reviews are kept, stale exceptions are dropped, and only findings without a
published fix are added. Fixable findings are listed and must be fixed instead. The one
exception is a third-party image whose newest stable release, verified by the reviewer and
named with --upstream-current, still bundles the vulnerable component: there is nothing to
upgrade to, so the finding is accepted and the published fix is recorded with it.

Reviews are periodic: --renew re-reviews every exception still observed, refreshing its review
dates, and drops any that now have a fix to adopt so they are fixed instead.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from security_policy import IDENTITY, identity, validate

REASONS = {
    "image:": "No upstream fix published for the pinned base; trusted local MOCK only, no public backend deployment.",
    "npm:development": "Development-only tooling, excluded from the production runtime; no patched release published.",
}
UPSTREAM_CURRENT = ("Patched component published, but the newest stable release of this upstream image "
                    "still bundles the vulnerable version; recheck on each upstream image release.")


def third_party_image(artifact: str) -> bool:
    # Images built here are named by role ("image:kafka"); pulled images keep their reference.
    return artifact.startswith("image:") and "/" in artifact


def reason_for(artifact: str) -> str:
    for prefix, reason in REASONS.items():
        if artifact.startswith(prefix):
            return reason
    raise ValueError(f"No reviewed acceptance rationale for {artifact}; fix the finding instead")


def propose(findings: list[dict], baseline: dict, reviewed: dt.date, review_by: dt.date,
            owner: str, upstream_current: frozenset[str] = frozenset(),
            renew: bool = False) -> tuple[dict, list[dict]]:
    if not all(third_party_image(artifact) for artifact in upstream_current):
        raise ValueError("Only third-party images can lack an upstream release; fix the others")
    existing = validate(baseline)
    exceptions, fixable, seen = [], [], set()
    for item in findings:
        key = identity(item)
        if key in seen:
            continue
        seen.add(key)
        entry = {**{field: item[field] for field in IDENTITY}, "severity": item["severity"],
                 "owner": owner, "reviewed": reviewed.isoformat(), "review_by": review_by.isoformat()}
        if key in existing and not renew:
            exceptions.append(existing[key])
        elif not item.get("fix_available"):
            reason = existing[key]["reason"] if key in existing else reason_for(item["artifact"])
            exceptions.append({**entry, "reason": reason})
        elif item["artifact"] in upstream_current:
            exceptions.append({**entry, "reason": UPSTREAM_CURRENT, "fix_available": True})
        else:
            fixable.append(item)
    exceptions.sort(key=lambda entry: tuple(str(entry[field]) for field in IDENTITY))
    return {"schema_version": 2, "exceptions": exceptions}, fixable


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--review-days", type=int, default=90)
    parser.add_argument("--owner", default="repository owner")
    parser.add_argument("--upstream-current", action="append", default=[], metavar="ARTIFACT",
                        help="third-party image verified to be the newest stable upstream release")
    parser.add_argument("--renew", action="store_true",
                        help="re-review existing exceptions that are still observed")
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_text())
    if evidence.get("schema_version") != 1 or not evidence.get("scanners_complete"):
        raise SystemExit("Security evidence incomplete; cannot propose exceptions")
    today = dt.datetime.now(dt.timezone.utc).date()
    proposal, fixable = propose(evidence["findings"], json.loads(args.baseline.read_text()),
                                today, today + dt.timedelta(days=args.review_days), args.owner,
                                frozenset(args.upstream_current), args.renew)
    args.output.write_bytes((json.dumps(proposal, indent=2) + "\n").encode())
    print(f"Exceptions proposed: {len(proposal['exceptions'])}; fixable, not accepted: {len(fixable)}")
    for item in fixable:
        print(f"FIX {item['artifact']} {item['component']} {item['version']} {item['advisory']}")
    raise SystemExit(1 if fixable else 0)


if __name__ == "__main__":
    main()
