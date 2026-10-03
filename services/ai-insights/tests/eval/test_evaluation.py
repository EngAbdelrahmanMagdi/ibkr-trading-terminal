"""Deterministic fixture-output evaluation; these results are not live-model quality."""

import json
from datetime import UTC, datetime
from pathlib import Path

from ai_insights.prompt import VERSION, article_input
from ai_insights.validation import validate_output


def test_golden_evaluation(raw, contracts):
    cases = json.loads((Path(__file__).parent / "cases.json").read_text())
    # Independent recorded fixture outputs; annotations never construct the classifications.
    outputs = json.loads((Path(__file__).parent / "outputs.json").read_text())
    true_positive = predicted = expected = 0
    counts = {
        "json_valid": 0,
        "sentiment_match": 0,
        "relevance_match": 0,
        "catalyst_match": 0,
        "unsupported_claims": 0,
    }
    for case in cases:
        raw["payload"]["headline"] = case["headline"]
        raw["payload"]["rawSummary"] = None
        raw["payload"]["symbols"] = case["symbols"]
        article_input(raw, 16384)
        sentiment, score, relevance, catalyst = outputs[case["name"]]
        output = {
            "summary": "Insufficient reliable information."
            if case.get("hostile")
            else case["headline"],
            "sentiment": sentiment,
            "sentimentScore": score,
            "relevanceScore": relevance,
            "catalysts": [{"type": catalyst, "description": case["headline"]}] if catalyst else [],
            "confidence": 0.4 if case.get("hostile") else 0.8,
            "evidence": [raw["payload"]["articleId"]],
            "flags": [],
        }
        result = validate_output(output, raw, contracts, 0.5)
        counts["json_valid"] += 1
        counts["sentiment_match"] += result["sentiment"] == case["sentiment"]
        counts["relevance_match"] += result["relevanceScore"] == case["relevance"]
        counts["catalyst_match"] += [c["type"] for c in result["catalysts"]] == (
            [case["catalyst"]] if case["catalyst"] else []
        )
        counts["unsupported_claims"] += "UNSUPPORTED_NUMBER" in result["flags"]
        actual_types = {c["type"] for c in result["catalysts"]}
        expected_types = {case["catalyst"]} if case["catalyst"] else set()
        true_positive += len(actual_types & expected_types)
        predicted += len(actual_types)
        expected += len(expected_types)
        assert result["evidence"] == [raw["payload"]["articleId"]]
        assert not any(word in result["summary"].lower() for word in case["forbiddenClaims"])
    assert counts["json_valid"] == len(cases)
    assert counts["unsupported_claims"] == 0
    print(
        json.dumps(
            {
                "dataset": "synthetic-golden.v1",
                "model": "fixture-outputs",
                "prompt": VERSION,
                "date": datetime.now(UTC).date().isoformat(),
                "catalyst_precision": true_positive / predicted if predicted else None,
                "catalyst_recall": true_positive / expected if expected else None,
                "cases": len(cases),
                **counts,
            }
        )
    )
