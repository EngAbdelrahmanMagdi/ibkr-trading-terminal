import json
import time
from decimal import Decimal

import httpx
import pytest
from pydantic import ValidationError

from ai_insights.config import Settings
from ai_insights.policy import Policy
from ai_insights.providers import OpenAIProvider, ProviderError


def test_public_and_private_fail_closed():
    assert Settings().provider == "FIXTURE"
    for changes in (
        {"provider": "OPENAI"},
        {"proxy_url": "http://proxy:8080"},
        {"daily_budget": "1"},
        {"attempts": 100},
        {"state_keys": 0},
    ):
        with pytest.raises(ValidationError):
            Settings(**changes)
    assert Policy(Settings()).reservation() == 0


def test_private_structured_http_without_network(tmp_path, contracts):
    key = tmp_path / "credential"
    key.write_text("test-placeholder")
    settings = Settings(
        provider="OPENAI",
        private_development=True,
        rights_verified=True,
        isolated_proxy_verified=True,
        model_policy_verified=True,
        daily_budget=Decimal("1"),
        proxy_url="http://restricted-proxy:8080",
        key_file=key,
        model="verified-model",
        model_version="verified-revision",
    )
    paths = []

    def request(req):
        assert req.url.host == "api.openai.com"
        paths.append(req.url.path)
        body = json.loads(req.content)
        if req.url.path.endswith("input_tokens"):
            return httpx.Response(200, json={"input_tokens": 100})
        assert body["store"] is False and "tools" not in body
        assert body["text"]["format"]["strict"] is True
        return httpx.Response(
            200,
            json={
                "status": "completed",
                "model": "verified-revision",
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": '{"summary":"sample"}'}],
                    }
                ],
            },
        )

    provider = OpenAIProvider(settings, contracts, httpx.MockTransport(request))
    assert provider.generate("{}", time.monotonic() + 5)["summary"] == "sample"
    assert paths == ["/v1/responses/input_tokens", "/v1/responses"]
    provider.close()
    provider = OpenAIProvider(
        settings, contracts, httpx.MockTransport(lambda _: httpx.Response(429))
    )
    with pytest.raises(ProviderError) as failure:
        provider.generate("{}", time.monotonic() + 5)
    assert failure.value.retry_after is None
    provider.close()


def test_rate_breaker_bounds():
    policy = Policy(Settings())
    policy.admit()
    with pytest.raises(ValueError):
        policy.admit()
    policy.failed("auth")
    policy.next_request = 0
    with pytest.raises(ValueError):
        policy.admit()
