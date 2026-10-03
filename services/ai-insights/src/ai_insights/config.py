"""Validated server configuration; private activation is explicit and fail-closed."""

import os
from decimal import Decimal
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: Literal["FIXTURE", "OPENAI"] = "FIXTURE"
    brokers: str = "kafka:29092"
    group: str = "ai-insights-news"
    transactional_id: str = "ai-insights-news-singleton"
    contracts_dir: Path = Path("/app/contracts/schemas")
    health_port: int = Field(default=8092, ge=1024, le=65535)
    state_keys: int = Field(default=20000, ge=10, le=100000)
    state_bytes: int = Field(default=64 * 1024 * 1024, ge=1024, le=256 * 1024 * 1024)
    state_ttl: int = Field(default=14 * 86400, ge=8 * 86400, le=90 * 86400)
    restore_seconds: int = Field(default=120, ge=10, le=300)
    restore_bytes: int = Field(default=512 * 1024 * 1024, ge=1024)
    input_bytes: int = Field(default=16384, ge=1024, le=65536)
    response_bytes: int = Field(default=131072, ge=1024, le=1048576)
    input_tokens: int = Field(default=4096, ge=256, le=8192)
    output_tokens: int = Field(default=1200, ge=128, le=4096)
    deadline: float = Field(default=30, ge=5, le=60)
    connect_timeout: float = Field(default=2, gt=0, le=5)
    read_timeout: float = Field(default=10, gt=0, le=20)
    attempts: int = Field(default=2, ge=1, le=2)
    requests_per_minute: int = Field(default=6, ge=1, le=60)
    admission_seconds: float = Field(default=2, ge=0, le=5)
    breaker_failures: int = Field(default=5, ge=1, le=20)
    breaker_cooldown: float = Field(default=60, ge=10, le=600)
    confidence_threshold: float = Field(default=0.5, ge=0, le=1)
    daily_budget: Decimal = Field(default=Decimal("0"), ge=0)
    private_development: bool = False
    rights_verified: bool = False
    isolated_proxy_verified: bool = False
    model_policy_verified: bool = False
    proxy_url: str = ""
    key_file: Path | None = None
    model: str = ""
    model_version: str = ""
    input_price: Decimal = Field(default=Decimal("0.10"), ge=0)
    output_price: Decimal = Field(default=Decimal("0.50"), ge=0)

    @model_validator(mode="after")
    def private_policy(self) -> Settings:
        if self.provider == "FIXTURE":
            if self.proxy_url or self.key_file or self.daily_budget:
                raise ValueError("fixture configuration must not contain private provider settings")
            return self
        proxy = urlsplit(self.proxy_url)
        if not all(
            (
                self.private_development,
                self.rights_verified,
                self.isolated_proxy_verified,
                self.model_policy_verified,
            )
        ):
            raise ValueError("private activation prerequisites are unverified")
        if (
            proxy.scheme not in {"http", "https"}
            or not proxy.hostname
            or proxy.username
            or proxy.password
            or proxy.path not in {"", "/"}
            or proxy.query
            or proxy.fragment
        ):
            raise ValueError("restricted proxy endpoint is required")
        if self.daily_budget <= 0 or not self.model or not self.model_version or not self.key_file:
            raise ValueError(
                "private activation requires approved budget, model revision and secret file"
            )
        return self

    @classmethod
    def environment(cls) -> Settings:
        return cls.model_validate(
            {
                name: os.environ[f"AI_{name.upper()}"]
                for name in cls.model_fields
                if f"AI_{name.upper()}" in os.environ
            }
        )
