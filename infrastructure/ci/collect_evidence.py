"""Convert complete scanner reports into exact policy evidence, retaining originals."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from image_context import RECIPES, context
import npm_evidence
import security_policy

ROLES = {
    "trading-terminal_trading-core_local.json": "trading-core",
    "trading-terminal_realtime-gateway_local.json": "realtime-gateway",
    "trading-terminal_ai-insights_local.json": "ai-insights",
    "trading-terminal_web_local.json": "web",
    "trading-terminal_kafka_local.json": "kafka",
    "trading-terminal_postgres_local.json": "postgres",
    "trading-terminal_redis_local.json": "redis",
    "trading-terminal_kafka-provisioner_local.json": "kafka-provisioner",
    "prom_prometheus_v3.13.4.json": "prom/prometheus:v3.13.4",
    "otel_opentelemetry-collector_0.161.0.json": "otel/opentelemetry-collector:0.161.0",
    "grafana_tempo_3.1.0.json": "grafana/tempo:3.1.0",
    "grafana_grafana_13.2.3.json": "grafana/grafana:13.2.3",
}


def collect(root: Path, directory: Path) -> dict:
    observed, images = [], {}
    for filename, role in ROLES.items():
        report = json.loads((directory / "images" / filename).read_text())
        architecture = report["Metadata"]["ImageConfig"]["architecture"]
        if architecture != "amd64":
            raise ValueError("Unreviewed runtime architecture")
        security_context = context(root, role)
        if role not in RECIPES:
            expected = security_context["base_images"][0].split("@", 1)[1]
            actual = report["Metadata"].get("RepoDigests") or []
            if not any(value.split("@", 1)[-1] == expected for value in actual):
                raise ValueError("Unreviewed external runtime image digest")
        observed.extend(security_policy.findings(report, security_context))
        images[role] = report["Metadata"]["ImageID"]
    observed.extend(npm_evidence.findings(root, json.loads((directory / "npm.json").read_text())))
    statuses = json.loads((directory / "scanner-status.json").read_text())
    if statuses["images"] not in {0, 1} or statuses["npm"] not in {0, 1}:
        raise ValueError("Scanner did not finish normally")
    return {"schema_version": 1, "scanners_complete": statuses["source"] == 0 and statuses["python"] == 0,
            "findings": observed, "built_images": images}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    evidence = collect(root, args.directory)
    (args.directory / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
