import time
from decimal import ROUND_CEILING, Decimal

from .config import Settings


class Policy:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.next_request = 0.0
        self.failures = 0
        self.open_until = 0.0
        self.auth_failed = False

    def admit(self) -> None:
        now = time.monotonic()
        if self.auth_failed or now < self.open_until:
            raise ValueError("circuit_open")
        if now < self.next_request:
            wait = self.next_request - now
            if wait > self.settings.admission_seconds:
                raise ValueError("rate_admission")
            # One synchronous admission wait, bounded by policy; no application waiting queue.
            time.sleep(wait)
            now = time.monotonic()
        self.next_request = now + 60 / self.settings.requests_per_minute

    def failed(self, category: str) -> None:
        self.auth_failed |= category == "auth"
        self.failures += 1
        if self.failures >= self.settings.breaker_failures:
            self.open_until = time.monotonic() + self.settings.breaker_cooldown

    def reservation(self) -> int:
        s = self.settings
        if s.provider == "FIXTURE":
            return 0
        price = (
            Decimal(s.input_tokens) * s.input_price + Decimal(s.output_tokens) * s.output_price
        ) / Decimal(1000000)
        return int((price * Decimal(1000000000)).to_integral_value(rounding=ROUND_CEILING))
