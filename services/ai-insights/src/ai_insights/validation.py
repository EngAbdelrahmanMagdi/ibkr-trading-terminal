import re
from typing import Any

from .contracts import Contracts
from .models import Insight


def validate_output(
    value: Any, event: dict[str, Any], contracts: Contracts, threshold: float
) -> dict[str, Any]:
    output = Insight.model_validate(value).model_dump()
    contracts.validate("news/news-insight.schema.json", output)
    p = event["payload"]
    if output["evidence"] != [p["articleId"]]:
        raise ValueError("evidence")
    source = p["headline"] + " " + (p["rawSummary"] or "")
    generated = output["summary"] + " " + " ".join(c["description"] for c in output["catalysts"])
    if re.search(
        r"\b(?:buy|sell|hold)\s+(?:orders?|shares?|stocks?|positions?)\b|price target",
        generated,
        re.I,
    ):
        raise ValueError("trading_instruction")
    flags = set(output["flags"])

    def numbers(text: str) -> set[str]:
        return set(re.findall(r"\d+(?:\.\d+)?", text.replace(",", "")))

    if numbers(generated) - numbers(source):
        flags.add("UNSUPPORTED_NUMBER")
    mentioned = set(re.findall(r"\$([A-Z][A-Z0-9.-]{0,11})\b", generated))
    if mentioned - set(p["symbols"]):
        flags.add("SYMBOL_MISMATCH")
    keywords = {
        "EARNINGS": r"earnings|revenue|profit",
        "GUIDANCE": r"guidance|outlook",
        "M_AND_A": r"merger|acqui",
        "ANALYST_RATING": r"analyst|upgrade|downgrade",
        "REGULATORY": r"regulat|investigat",
        "PRODUCT": r"product|launch",
        "MANAGEMENT": r"chief|management|ceo",
        "MACRO": r"inflation|rates|econom",
    }
    if output["confidence"] < threshold or any(
        c["type"] in keywords and not re.search(keywords[c["type"]], source, re.I)
        for c in output["catalysts"]
    ):
        flags.add("LOW_CONFIDENCE")
    output["flags"] = sorted(flags)
    contracts.validate("news/news-insight.schema.json", output)
    return output
