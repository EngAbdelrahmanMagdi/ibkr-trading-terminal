"""Immutable CI base inputs and security-relevant recipe identities."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

from security_policy import recipe_hash

RECIPES = {
    "trading-core": ["services/trading-core/Dockerfile", "services/trading-core/pom.xml"],
    "realtime-gateway": ["services/ibkr-realtime-gateway/Dockerfile", "services/ibkr-realtime-gateway/go.mod", "services/ibkr-realtime-gateway/go.sum"],
    "ai-insights": ["services/ai-insights/Dockerfile", "services/ai-insights/requirements.txt"],
    "web": ["apps/web/Dockerfile", "apps/web/package.json", "apps/web/package-lock.json"],
    "postgres": ["infrastructure/postgres/Dockerfile"],
    "redis": ["infrastructure/redis/Dockerfile"],
    "kafka": ["infrastructure/kafka/Dockerfile", "infrastructure/kafka/security-patches.pom.xml"],
    "kafka-provisioner": ["infrastructure/kafka/Dockerfile", "infrastructure/kafka/security-patches.pom.xml"],
}


def context(root: Path, role: str) -> dict:
    lock = json.loads((root / "security/image-inputs.json").read_text())
    recipes = RECIPES.get(role, [])
    if recipes:
        references = re.findall(r"(?mi)^FROM\s+(\S+)", (root / recipes[0]).read_text())
        bases = sorted({lock["bases"][reference] for reference in references if reference in lock["bases"]})
    else:
        bases = [lock["bases"][role]]
    if not bases or any("@sha256:" not in base for base in bases):
        raise ValueError("Immutable bases required")
    return {"artifact": f"image:{role}", "architecture": lock["architecture"],
            "recipe_sha256": recipe_hash(root, recipes), "base_images": bases}


def pin_dockerfile(root: Path, role: str, output: Path) -> None:
    lock = json.loads((root / "security/image-inputs.json").read_text())
    text = (root / RECIPES[role][0]).read_text()
    stages = set()
    lines = []
    for line in text.splitlines():
        match = re.match(r"(?i)^FROM\s+(\S+)(.*)$", line)
        if match:
            reference, suffix = match.groups()
            if reference not in stages:
                if reference not in lock["bases"]:
                    raise ValueError("Unpinned external base")
                line = f"FROM {lock['bases'][reference]}{suffix}"
            alias = re.search(r"(?i)\s+AS\s+(\S+)", suffix)
            if alias:
                stages.add(alias[1])
        lines.append(line)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=RECIPES)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    pin_dockerfile(Path(__file__).resolve().parents[2], args.role, args.output)
