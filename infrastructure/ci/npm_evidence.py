"""Resolve npm advisory chains while keeping the original audit report intact."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from security_policy import recipe_hash


def has_fixed_release(vulnerable_range: str) -> bool:
    # Advisories name the first patched release as an exclusive bound ("<1.2.2"); an
    # inclusive bound ("<=3.0.3") or open range ("*") means no patched release exists yet.
    # Merged hyphen ranges ("1.0.0 - 1.2.1") are treated as fixable to fail closed.
    return re.search(r"<(?!=)", vulnerable_range) is not None or " - " in vulnerable_range


def findings(root: Path, report: dict[str, Any]) -> list[dict[str, Any]]:
    import json

    if report.get("error") or report.get("auditReportVersion") != 2 or not isinstance(report.get("vulnerabilities"), dict):
        raise ValueError("Incomplete npm scanner evidence")
    lock = json.loads((root / "apps/web/package-lock.json").read_text())
    vulnerabilities = report["vulnerabilities"]
    result = {}

    def visit(name: str, seen: set[str]) -> None:
        if name in seen:
            return
        entry = vulnerabilities[name]
        for advisory in entry["via"]:
            if isinstance(advisory, str):
                visit(advisory, seen | {name})
                continue
            if advisory["severity"] not in {"high", "critical"}:
                continue
            for node in entry["nodes"]:
                package = lock["packages"][node]
                identity = advisory["url"].rsplit("/", 1)[-1]
                item = {"artifact": "npm:development" if package.get("dev") else "npm:production",
                        "architecture": "all", "recipe_sha256": recipe_hash(root, ["apps/web/package-lock.json"]),
                        "base_images": [], "advisory": identity, "severity": advisory["severity"].upper(),
                        "component": name, "version": package["version"], "target": node,
                        "fix_available": has_fixed_release(advisory.get("range", "*"))}
                result[(identity, node)] = item

    for name, entry in vulnerabilities.items():
        if entry["severity"] in {"high", "critical"}:
            visit(name, set())
    if any(entry["severity"] in {"high", "critical"} for entry in vulnerabilities.values()) and not result:
        raise ValueError("High/Critical npm findings lack complete advisory evidence")
    return list(result.values())
