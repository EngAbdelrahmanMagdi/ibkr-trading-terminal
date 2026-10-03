"""Neutral provider boundary. No tools, scraping, ambient proxies or payload logging."""

import json
import socket
import time
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

from . import metrics
from .config import Settings
from .contracts import Contracts
from .prompt import INSTRUCTIONS


class ProviderError(Exception):
    def __init__(self, category: str, retry_after: float | None = None) -> None:
        super().__init__(category)
        self.category = category
        self.retry_after = retry_after


class Provider(Protocol):
    model: str
    revision: str

    def generate(self, data: str, deadline: float) -> dict[str, Any]: ...


class FixtureProvider:
    model = "synthetic-news.v1"
    revision = "1"

    def generate(self, data: str, deadline: float) -> dict[str, Any]:
        output: dict[str, Any] = json.loads(
            (Path(__file__).parent / "fixtures/synthetic-insights.json").read_text()
        )
        output["evidence"] = [json.loads(data)["articleId"]]
        return output


class OpenAIProvider:
    def __init__(
        self, settings: Settings, contracts: Contracts, transport: httpx.BaseTransport | None = None
    ) -> None:
        self.settings = settings
        self.model = settings.model
        self.revision = settings.model_version
        # Prove restricted proxy connectivity; never manufacture a network route.
        if transport is None:
            routes = Path("/proc/net/route").read_text().splitlines()[1:]
            if any(row.split()[1] == "00000000" for row in routes if len(row.split()) >= 3):
                raise ValueError("direct_egress_route")
            ipv6 = Path("/proc/net/ipv6_route")
            if ipv6.exists() and any(
                row.split()[0] == "0" * 32 and row.split()[1] == "00" and row.split()[-1] != "lo"
                for row in ipv6.read_text().splitlines()
                if len(row.split()) >= 10
            ):
                raise ValueError("direct_ipv6_egress_route")
            endpoint = urlsplit(settings.proxy_url)
            with socket.create_connection(
                (
                    endpoint.hostname or "",
                    endpoint.port or (443 if endpoint.scheme == "https" else 80),
                ),
                timeout=2,
            ):
                pass
        if settings.key_file is None:
            raise ValueError("secret_file")
        secret = settings.key_file.read_text().strip()
        if not secret or len(secret) > 512 or any(c.isspace() for c in secret):
            raise ValueError("secret_file")
        self.client = httpx.Client(
            proxy=settings.proxy_url if transport is None else None,
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            headers={"Authorization": f"Bearer {secret}"},
            timeout=httpx.Timeout(settings.read_timeout, connect=settings.connect_timeout),
        )
        self.schema = contracts.provider_schema()

    def request(self, path: str, payload: dict[str, Any], deadline: float) -> dict[str, Any]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ProviderError("timeout")
        try:
            with self.client.stream(
                "POST",
                "https://api.openai.com/v1/responses" + path,
                json=payload,
                timeout=min(self.settings.read_timeout, remaining),
            ) as r:
                if r.status_code in {401, 403}:
                    raise ProviderError("auth")
                if r.status_code == 429:
                    raw = r.headers.get("Retry-After", "")
                    raise ProviderError("rate", float(raw) if raw.isdigit() else None)
                if r.status_code >= 500:
                    raise ProviderError("transient")
                if r.status_code != 200:
                    raise ProviderError("rejected")
                body = bytearray()
                for chunk in r.iter_bytes():
                    if time.monotonic() >= deadline:
                        raise ProviderError("timeout")
                    body.extend(chunk)
                    if len(body) > self.settings.response_bytes:
                        raise ProviderError("response_size")
                result: dict[str, Any] = json.loads(body)
                return result
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            raise ProviderError("transient") from e

    def generate(self, data: str, deadline: float) -> dict[str, Any]:
        base = {"model": self.revision, "instructions": INSTRUCTIONS, "input": data}
        count = self.request("/input_tokens", base, deadline)
        tokens = count.get("input_tokens")
        if (
            not isinstance(tokens, int)
            or isinstance(tokens, bool)
            or not 0 < tokens <= self.settings.input_tokens
        ):
            raise ProviderError("input_tokens")
        result = self.request(
            "",
            {
                **base,
                "store": False,
                "max_output_tokens": self.settings.output_tokens,
                "reasoning": {"effort": "none"},
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "news_insight",
                        "strict": True,
                        "schema": self.schema,
                    }
                },
            },
            deadline,
        )
        usage = result.get("usage", {})
        if isinstance(usage, dict):
            for direction in ("input", "output"):
                amount = usage.get(direction + "_tokens")
                if (
                    isinstance(amount, int)
                    and not isinstance(amount, bool)
                    and 0 <= amount <= 1000000
                ):
                    metrics.TOKENS.labels(direction).inc(amount)
        if result.get("status") != "completed":
            raise ProviderError("incomplete")
        if result.get("model") != self.revision:
            raise ProviderError("model_revision")
        texts = [
            c["text"]
            for item in result.get("output", [])
            if item.get("type") == "message"
            for c in item.get("content", [])
            if c.get("type") == "output_text"
        ]
        if len(texts) != 1:
            raise ProviderError("refusal")
        output: dict[str, Any] = json.loads(texts[0])
        return output

    def close(self) -> None:
        self.client.close()
