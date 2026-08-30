"""수집기 설정. 값은 .env에서 온다 (커밋되지 않는다)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _split(v: str) -> list[str]:
    return [s.strip() for s in v.split(",") if s.strip()]


@dataclass(frozen=True)
class Config:
    client_id: str
    client_secret: str

    api_base: str = "https://openapi.tossinvest.com"
    ws_url: str = "wss://openapi-ws.tossinvest.com/ws/v1"

    # 범위를 넓히지 않는다 — 종목은 소수만.
    symbols: list[str] = field(default_factory=lambda: ["005930", "000660"])

    bootstrap_servers: str = "localhost:9092"
    topic: str = "trades.raw"

    # 문서 기준: 60초마다 PING, 180초 무활동 시 서버가 끊는다.
    ping_interval_s: float = 60.0
    # 그 절반에서 수신이 멎으면 죽은 연결로 보고 먼저 끊는다.
    # 서버가 끊어주기를 기다리면 그만큼 유실 구간이 길어진다.
    recv_timeout_s: float = 90.0

    # 토큰 유효기간 24h. 만료 여유를 두고 미리 갱신한다.
    token_refresh_margin_s: float = 3600.0

    metrics_port: int = 9200
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        cid = os.getenv("TOSS_CLIENT_ID", "").strip()
        sec = os.getenv("TOSS_CLIENT_SECRET", "").strip()
        if not cid or not sec:
            raise SystemExit(
                "TOSS_CLIENT_ID / TOSS_CLIENT_SECRET 가 없습니다. "
                ".env.example 을 .env 로 복사해 채우세요."
            )
        kw: dict = {"client_id": cid, "client_secret": sec}
        if v := os.getenv("TOSS_API_BASE"):
            kw["api_base"] = v
        if v := os.getenv("TOSS_WS_URL"):
            kw["ws_url"] = v
        if v := os.getenv("SYMBOLS"):
            kw["symbols"] = _split(v)
        if v := os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            kw["bootstrap_servers"] = v
        if v := os.getenv("KAFKA_TRADES_TOPIC"):
            kw["topic"] = v
        if v := os.getenv("METRICS_PORT"):
            kw["metrics_port"] = int(v)
        if v := os.getenv("LOG_LEVEL"):
            kw["log_level"] = v.upper()
        return cls(**kw)

    def redacted(self) -> dict:
        """로그용. 자격 증명은 절대 남기지 않는다."""
        return {
            "api_base": self.api_base,
            "ws_url": self.ws_url,
            "symbols": self.symbols,
            "bootstrap_servers": self.bootstrap_servers,
            "topic": self.topic,
            "client_id": self.client_id[:4] + "…" if self.client_id else "",
        }
