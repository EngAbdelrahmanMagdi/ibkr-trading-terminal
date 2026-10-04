"""Evaluate complete vulnerability evidence without filtering scanner reports."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

IDENTITY = (
    "artifact", "architecture", "recipe_sha256", "base_images",
    "advisory", "severity", "component", "version", "target",
)


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
                               "version": item["InstalledVersion"], "target": location})
    return result


def evaluate(observed: list[dict[str, Any]], baseline: dict[str, Any],
             today: dt.date) -> dict[str, Any]:
    if baseline.get("schema_version") != 1 or not isinstance(baseline.get("exceptions"), list):
        raise ValueError("Invalid exception baseline")
    accepted, rejected = [], []
    for exception in baseline["exceptions"]:
        if any(key not in exception for key in (*IDENTITY, "expires", "reason", "owner")):
            raise ValueError("Incomplete exception identity")
        if not exception["reason"] or not exception["owner"]:
            raise ValueError("Exception disposition missing")
        dt.date.fromisoformat(exception["expires"])
    for item in observed:
        match = next((entry for entry in baseline["exceptions"]
                      if all(item.get(key) == entry[key] for key in IDENTITY)), None)
        if match and today <= dt.date.fromisoformat(match["expires"]):
            accepted.append(item)
        else:
            rejected.append({**item, "policy_reason": "expired" if match else "unaccepted-context"})
    return {"policy_pass": not rejected, "accepted_unresolved": accepted, "rejected": rejected,
            "runtime_image_delivery_allowed": not any(
                item["severity"] == "CRITICAL" and item["artifact"].startswith("image:")
                for item in observed)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_text())
    if evidence.get("schema_version") != 1 or not evidence.get("scanners_complete"):
        raise SystemExit("Security evidence incomplete; scanner failures cannot be accepted")
    outcome = evaluate(evidence["findings"], json.loads(args.baseline.read_text()),
                       dt.datetime.now(dt.timezone.utc).date())
    outcome["built_images"] = evidence.get("built_images", {})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(outcome, indent=2) + "\n")
    print(f"Accepted unresolved: {len(outcome['accepted_unresolved'])}; "
          f"unaccepted/expired: {len(outcome['rejected'])}; "
          f"runtime image delivery allowed: {outcome['runtime_image_delivery_allowed']}")
    raise SystemExit(0 if outcome["policy_pass"] else 1)


if __name__ == "__main__":
    main()
