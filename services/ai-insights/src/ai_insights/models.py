"""Typed model output. Canonical schemas remain the wire authority."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

Score = Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]


class Catalyst(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    type: Literal[
        "EARNINGS",
        "GUIDANCE",
        "M_AND_A",
        "ANALYST_RATING",
        "REGULATORY",
        "PRODUCT",
        "MANAGEMENT",
        "MACRO",
        "OTHER",
    ]
    description: str = Field(min_length=1, max_length=300)


class Insight(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    summary: str = Field(min_length=1, max_length=600)
    sentiment: Literal["POSITIVE", "NEGATIVE", "NEUTRAL", "MIXED"]
    sentimentScore: float = Field(ge=-1, le=1, allow_inf_nan=False)
    relevanceScore: Score
    catalysts: list[Catalyst] = Field(max_length=5)
    confidence: Score
    evidence: list[str] = Field(min_length=1, max_length=1)
    flags: list[Literal["LOW_CONFIDENCE", "UNSUPPORTED_NUMBER", "SYMBOL_MISMATCH"]]
