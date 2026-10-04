"""Validate destination-neutral delivery files and preserve detected component SBOMs."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import tarfile
import uuid

FORBIDDEN = {"secrets", ".local", "docs", ".claude", "recordings", "test-results", "playwright-report"}


def validate_files(output: Path) -> None:
    for path in output.rglob("*"):
        if path.is_symlink():
            raise ValueError("Delivery cannot contain symlinks")
        parts = set(path.relative_to(output).parts)
        if parts & FORBIDDEN or path.name in {".env", "CLAUDE.md", "publication.json", "wrangler.json"}:
            raise ValueError("Local-only content in delivery")
        if path.suffix.lower() in {".webm", ".mp4", ".pem", ".key", ".p12"}:
            raise ValueError("Media or credentials in delivery")
        if path.name.startswith(("source-", "contracts-", "showcase-source-")) and path.name.endswith(".tar.gz"):
            with tarfile.open(path) as archive:
                for member in archive:
                    parts = set(Path(member.name).parts)
                    if parts & (FORBIDDEN | {"..", ".env", "CLAUDE.md"}) or member.name.startswith("/") or member.issym() or member.islnk():
                        raise ValueError("Unsafe or local-only archive content")


def image_delivery_allowed(evidence: dict, decision: dict) -> bool:
    if evidence.get("scanners_complete") is not True or decision.get("policy_pass") is not True:
        raise ValueError("Delivery requires completed scans and passing exception policy")
    return decision.get("runtime_image_delivery_allowed") is True and not any(
        item["severity"] == "CRITICAL" and item["artifact"].startswith("image:") for item in evidence["findings"])


def sbom(report: dict) -> dict:
    digest = report["Metadata"]["ImageID"]
    packages = {}
    for result in report.get("Results", []):
        for item in result.get("Packages", []):
            identity = (result.get("Type", "unknown"), item["Name"], item["Version"])
            packages[identity] = {"name": item["Name"], "versionInfo": item["Version"],
                                  "downloadLocation": "NOASSERTION", "filesAnalyzed": False,
                                  "licenseConcluded": "NOASSERTION", "licenseDeclared": "NOASSERTION",
                                  "copyrightText": "NOASSERTION"}
    entries = []
    for identity, package in sorted(packages.items()):
        key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
        entries.append({"SPDXID": "SPDXRef-" + key, **package})
    return {"spdxVersion": "SPDX-2.3", "dataLicense": "CC0-1.0", "SPDXID": "SPDXRef-DOCUMENT",
            "name": "Detected runtime components " + digest,
            "documentNamespace": "urn:uuid:" + str(uuid.uuid4()),
            "creationInfo": {"creators": ["Tool: MarketPulse delivery"],
                             "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                             "comment": "Trivy detected packages only; unidentified native components are not covered."},
            "packages": entries, "relationships": [{"spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES", "relatedSpdxElement": item["SPDXID"]} for item in entries]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    output = args.directory
    target = output / "sbom"
    target.mkdir(exist_ok=True)
    for report in output.glob("security-evidence/run-*/images/*.json"):
        target.joinpath(report.stem + ".spdx.json").write_text(json.dumps(sbom(json.loads(report.read_text())), indent=2) + "\n")
    if not list(target.glob("*.spdx.json")):
        raise SystemExit("Missing complete runtime component evidence")
    validate_files(output)
