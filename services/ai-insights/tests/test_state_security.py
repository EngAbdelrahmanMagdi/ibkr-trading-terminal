import json
from copy import deepcopy
from uuid import NAMESPACE_URL, uuid5

import pytest
from jsonschema.exceptions import ValidationError

from ai_insights.state import State


def test_completed_state_requires_saved_event(settings, contracts, raw):
    state = State(settings, contracts)
    article = raw["payload"]["articleId"]
    identity = str(uuid5(NAMESPACE_URL, article + ":" + "a" * 64))
    data = {
        "articleId": article,
        "processingIdentity": "a" * 64,
        "status": "DONE",
        "reservedNanodollars": 0,
        "budgetDay": "2026-10-04",
    }
    with pytest.raises(ValidationError):
        state.record("ARTICLE", identity, data)


def test_restore_rejects_forged_article_identity(settings, contracts, raw):
    state = State(settings, contracts)
    article = raw["payload"]["articleId"]
    identity = str(uuid5(NAMESPACE_URL, article + ":" + "a" * 64))
    record = state.record(
        "ARTICLE",
        identity,
        {
            "articleId": article,
            "processingIdentity": "a" * 64,
            "status": "STARTED",
            "reservedNanodollars": 0,
            "budgetDay": "2026-10-04",
        },
    )
    forged = deepcopy(record)
    forged["identity"] = str(uuid5(NAMESPACE_URL, "foreign"))
    with pytest.raises(ValueError, match="state_article_identity"):
        state.apply({"ARTICLE:" + forged["identity"]: forged})
    assert not state.values


def test_invalid_batch_is_atomic_and_caller_cannot_mutate_saved_state(settings, contracts):
    state = State(settings, contracts)
    record = state.record("BUDGET", "2026-10-04", {"reservedNanodollars": 10})
    with pytest.raises(ValueError, match="state_key"):
        state.apply({"BUDGET:2026-10-04": record, "wrong-key": record})
    assert not state.values
    state.apply({"BUDGET:2026-10-04": record})
    record["data"]["reservedNanodollars"] = 0
    read = state.get("BUDGET:2026-10-04")
    assert read["reservedNanodollars"] == 10
    read["reservedNanodollars"] = 0
    assert state.get("BUDGET:2026-10-04")["reservedNanodollars"] == 10


def test_incremental_capacity_matches_full_accounting(settings, contracts):
    state = State(settings, contracts)
    for day in range(1, 20):
        key = f"BUDGET:2026-10-{day:02}"
        record = state.record("BUDGET", key.split(":")[1], {"reservedNanodollars": day})
        state.apply({key: record})
        state.apply(
            {key: state.record("BUDGET", key.split(":")[1], {"reservedNanodollars": day * 1000})}
        )
        if day > 1:
            state.apply({f"BUDGET:2026-10-{day - 1:02}": None})
        expected = sum(
            len(k.encode()) + len(json.dumps(v).encode()) for k, v in state.values.items()
        )
        assert state._bytes == expected
    before = deepcopy(state.values)
    bound = state._bytes
    state.settings.state_bytes = bound
    with pytest.raises(ValueError, match="state_capacity"):
        state.apply(
            {"BUDGET:2026-11-01": state.record("BUDGET", "2026-11-01", {"reservedNanodollars": 1})}
        )
    assert state.values == before
    assert state._bytes == bound
