import hashlib
import json
import re
from pathlib import Path
from typing import Any

VERSION = "news-insight.v1"
INSTRUCTIONS = (Path(__file__).parent / "prompts" / f"{VERSION}.md").read_text()


def article_input(event: dict[str, Any], maximum: int) -> str:
    p = event["payload"]
    data = {
        k: p[k] for k in ("articleId", "headline", "source", "publishedAt", "rawSummary", "symbols")
    }
    data["primarySymbol"] = event["symbol"]
    for k, v in data.items():
        if isinstance(v, str):
            data[k] = re.sub(r"[\x00-\x1f\x7f]", " ", v)
    serialized = json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if len(serialized.encode()) + len(INSTRUCTIONS.encode()) > maximum:
        raise ValueError("input_size")
    return serialized


def identity(event: dict[str, Any], model: str, revision: str) -> str:
    p = event["payload"]
    context = [
        p["contentHash"],
        p["providerId"].split(":", 1)[0],
        event["symbol"],
        sorted(p["symbols"]),
        VERSION,
        hashlib.sha256(INSTRUCTIONS.encode()).hexdigest(),
        model,
        revision,
        "validation.v1",
    ]
    return hashlib.sha256(json.dumps(context, separators=(",", ":")).encode()).hexdigest()
