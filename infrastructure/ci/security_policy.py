"""Evaluate complete vulnerability evidence without filtering scanner reports.

Gate mode (pull requests, pushes, manual runs) fails on any finding without a reviewed
exception. Accepted findings that need attention again (review date reached, upstream fix
published, severity changed) are reported but only fail monitor mode, which runs on the
weekly schedule so reviews never break unrelated pull requests.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any

# What was reviewed: the vulnerable package in a specific artifact. Build recipes and base
# digests are recorded as evidence but are not identity, so rebuilds keep their review.
IDENTITY = ("artifact", "architecture", "advisory", "component", "version", "target")
DISPOSITION = ("severity", "owner", "reason", "reviewed", "review_by")
MODES = ("gate", "monitor")


def recipe_hash(root: Path, files: list[str]) -> str:
    digest = hashlib.sha256()
    for name in sorted(files):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update((root / name).read_bytes().replace(b"\r\n", b"\n"))
        digest.update(b"\0")
    return digest.hexdigest()


def findings(report: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    if report.get("SchemaVersion") != 2 or not isinstance(report.get("Results"), list):
        raise ValueError("Missing or unsupported complete Trivy report")
    if not report.get("Metadata", {}).get("ImageID"):
        raise ValueError("Image identity missing from scanner evidence")
    result = []
    for target in report["Results"]:
        for item in target.get("Vulnerabilities") or []:
            if item["Severity"] in {"HIGH", "CRITICAL"}:
                # Trivy embeds a mutable local tag in OS target labels. Bind instead to
                # the distribution identity; application rebuilds keep this context.
                location = target["Target"]
                if target.get("Class") == "os-pkgs":
                    os_info = report["Metadata"].get("OS", {})
                    location = f"{target['Type']}:{os_info['Name']}"
                result.append({**context, "advisory": item["VulnerabilityID"],
                               "severity": item["Severity"], "component": item["PkgName"],
                               "version": item["InstalledVersion"], "target": location,
                               "fix_available": bool(item.get("FixedVersion"))})
    return result


def identity(entry: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(entry[key] for key in IDENTITY)


def validate(baseline: dict[str, Any]) -> dict[tuple[Any, ...], dict[str, Any]]:
    if baseline.get("schema_version") != 2 or not isinstance(baseline.get("exceptions"), list):
        raise ValueError("Invalid exception baseline")
    exceptions: dict[tuple[Any, ...], dict[str, Any]] = {}
    for exception in baseline["exceptions"]:
        if any(key not in exception for key in (*IDENTITY, *DISPOSITION)):
            raise ValueError("Incomplete exception identity")
        if not exception["reason"] or not exception["owner"]:
            raise ValueError("Exception disposition missing")
        if dt.date.fromisoformat(exception["review_by"]) < dt.date.fromisoformat(exception["reviewed"]):
            raise ValueError("Exception review date precedes its review")
        key = identity(exception)
        if key in exceptions:
            raise ValueError("Duplicate exception identity")
        exceptions[key] = exception
    return exceptions


def evaluate(observed: list[dict[str, Any]], baseline: dict[str, Any],
             today: dt.date, mode: str = "gate") -> dict[str, Any]:
    if mode not in MODES:
        raise ValueError("Unknown policy mode")
    exceptions = validate(baseline)
    accepted, rejected, needs_review, matched = [], [], [], set()
    for item in observed:
        key = identity(item)
        match = exceptions.get(key)
        if match is None:
            reason = "fix-available" if item.get("fix_available") else "unaccepted"
            rejected.append({**item, "policy_reason": reason})
            continue
        matched.add(key)
        accepted.append(item)
        reasons = []
        # A fix recorded at review time had no upstream release to adopt; only a fix
        # published since then needs another look before the review date.
        if item.get("fix_available") and not match.get("fix_available"):
            reasons.append("fix-available")
        if item["severity"] != match["severity"]:
            reasons.append("severity-changed")
        if today > dt.date.fromisoformat(match["review_by"]):
            reasons.append("review-due")
        if reasons:
            needs_review.append({**item, "review_reasons": reasons})
    stale = [exception for key, exception in exceptions.items() if key not in matched]
    failed = rejected + (needs_review if mode == "monitor" else [])
    return {"mode": mode, "policy_pass": not failed, "accepted_unresolved": accepted,
            "rejected": rejected, "needs_review": needs_review, "stale_exceptions": stale,
            "runtime_image_delivery_allowed": not any(
                item["severity"] == "CRITICAL" and item["artifact"].startswith("image:")
                for item in observed)}


def summary(outcome: dict[str, Any]) -> str:
    """Markdown for the run summary; findings are reported there, never as comments."""
    lines = [f"### Security policy ({outcome['mode']}): {'pass' if outcome['policy_pass'] else 'fail'}",
             f"Accepted unresolved: {len(outcome['accepted_unresolved'])} · "
             f"unaccepted: {len(outcome['rejected'])} · needs review: {len(outcome['needs_review'])} · "
             f"stale exceptions: {len(outcome['stale_exceptions'])}"]
    sections = (("Unaccepted findings", outcome["rejected"], "policy_reason"),
                ("Accepted findings needing review", outcome["needs_review"], "review_reasons"))
    for title, items, field in sections:
        if items:
            lines += ["", f"#### {title}", "| Reason | Artifact | Component | Version | Advisory |",
                      "|---|---|---|---|---|"]
            for item in items:
                reason = item[field] if isinstance(item[field], str) else ", ".join(item[field])
                lines.append(f"| {reason} | {item['artifact']} | {item['component']} | "
                             f"{item['version']} | {item['advisory']} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=MODES, default="gate")
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_text())
    if evidence.get("schema_version") != 1 or not evidence.get("scanners_complete"):
        raise SystemExit("Security evidence incomplete; scanner failures cannot be accepted")
    outcome = evaluate(evidence["findings"], json.loads(args.baseline.read_text()),
                       dt.datetime.now(dt.timezone.utc).date(), args.mode)
    outcome["built_images"] = evidence.get("built_images", {})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(outcome, indent=2) + "\n")
    print(f"Mode: {outcome['mode']}; accepted unresolved: {len(outcome['accepted_unresolved'])}; "
          f"unaccepted: {len(outcome['rejected'])}; needs review: {len(outcome['needs_review'])}; "
          f"stale exceptions: {len(outcome['stale_exceptions'])}; "
          f"runtime image delivery allowed: {outcome['runtime_image_delivery_allowed']}")
    for item in outcome["rejected"]:
        print(f"UNACCEPTED {item['policy_reason']}: {item['artifact']} {item['component']} "
              f"{item['version']} {item['advisory']} {item['severity']}")
    for item in outcome["needs_review"]:
        print(f"REVIEW {','.join(item['review_reasons'])}: {item['artifact']} "
              f"{item['component']} {item['version']} {item['advisory']}")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as report:
            report.write(summary(outcome))
    raise SystemExit(0 if outcome["policy_pass"] else 1)


if __name__ == "__main__":
    main()
