import json
from pathlib import Path

import pytest

from ai_insights.config import Settings
from ai_insights.contracts import Contracts

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def contracts():
    return Contracts(ROOT / "contracts/schemas")


@pytest.fixture
def settings():
    return Settings(contracts_dir=ROOT / "contracts/schemas")


@pytest.fixture
def raw():
    from ai_insights.state import utc

    value = json.loads(
        (ROOT / "tests/contract/fixtures/events/news-raw/valid/ingested.json").read_text()
    )
    value["occurredAt"] = utc()
    return value
