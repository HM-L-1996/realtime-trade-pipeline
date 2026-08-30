"""토스 WebSocket 체결 → Kafka.

재연결·백오프·토큰 갱신을 여기서 묶는다.
소스가 lossy 하므로 '끊긴 구간'을 conn_id 로 남겨 나중에 유실을 추적할 수 있게 한다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import time

import httpx
from aiokafka import AIOKafkaProducer
from websockets.exceptions import ConnectionClosed, InvalidStatus

from .auth import TokenError, TokenProvider
from .config import Config
from .metrics import (
    CONNECTED,
    OUT_OF_ORDER,
    PRODUCE_ERRORS,
    RECONNECTS,
    SOURCE_LAG,
    TRADES,
    serve,
)
from .sequencing import Sequencer
from .toss_ws import TossTradeStream

log = logging.getLogger("ingester")

BACKOFF_START = 1.0
BACKOFF_MAX = 30.0


def _record(t: dict, seq_in_ms: int, ingest_ms: int) -> bytes:
    return json.dumps(
        {
            "symbol": t["symbol"],
            "event_ms": t["event_ms"],
            "seq_in_ms": seq_in_ms,
            "ingest_ms": ingest_ms,
            "price": t["price"],
            "volume": t["volume"],
            "currency": t["currency"],
            "conn_id": t["conn_id"],
            "recv_seq": t["recv_seq"],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


async def run(cfg: Config) -> None:
    serve(cfg.metrics_port)
    log.info("설정 %s", cfg.redacted())

    tokens = TokenProvider(cfg.api_base, cfg.client_id, cfg.client_secret,
                           cfg.token_refresh_margin_s)
    stream = TossTradeStream(cfg.ws_url, cfg.symbols,
                             cfg.ping_interval_s, cfg.recv_timeout_s)
    sequencer = Sequencer()

    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.bootstrap_servers,
        # 순서와 무유실을 우선한다. 수집기가 병목이 되면 그건 그것대로 관측 대상이다.
        acks="all",
        enable_idempotence=True,
        linger_ms=20,
        compression_type="lz4",
    )
    await producer.start()

    stop = asyncio.Event()

    def _signal(*_: object) -> None:
        log.info("종료 신호 수신")
        stop.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _signal)
        except NotImplementedError:
            signal.signal(s, _signal)  # Windows

    backoff = BACKOFF_START
    try:
        async with httpx.AsyncClient() as http:
            while not stop.is_set():
                reason = "unknown"
                try:
                    # 재연결 시점에 토큰이 만료돼 있으면 여기서 걸린다.
                    # WebSocket 은 핸드셰이크에서만 토큰을 보므로 이 갱신이 유일한 방어선이다.
                    token = await tokens.get(http)

                    CONNECTED.set(1)
                    async for trade in stream.stream(token):
                        seq, ooo = sequencer.assign(trade["symbol"], trade["event_ms"])
                        ingest_ms = int(time.time() * 1000)

                        SOURCE_LAG.observe(max(0.0, (ingest_ms - trade["event_ms"]) / 1000.0))
                        if ooo:
                            OUT_OF_ORDER.labels(symbol=trade["symbol"]).inc()

                        try:
                            await producer.send_and_wait(
                                cfg.topic,
                                key=trade["symbol"].encode(),  # 종목별 순서 보장
                                value=_record(trade, seq, ingest_ms),
                            )
                            TRADES.labels(symbol=trade["symbol"]).inc()
                        except Exception:
                            PRODUCE_ERRORS.inc()
                            log.exception("Kafka 전송 실패")

                        backoff = BACKOFF_START  # 정상 수신이 있었으면 백오프 초기화
                        if stop.is_set():
                            break

                    reason = "stream-ended"
                except ConnectionClosed:
                    reason = "closed"
                except InvalidStatus as e:
                    # 401 이면 토큰 문제. 다음 회차에 강제 재발급한다.
                    reason = f"handshake-{e.response.status_code}"
                    if e.response.status_code in (401, 403):
                        try:
                            await tokens.get(http, force=True)
                        except TokenError:
                            log.exception("토큰 강제 재발급 실패")
                except TokenError:
                    reason = "token"
                    log.exception("토큰 오류")
                except Exception:
                    reason = "error"
                    log.exception("스트림 예외")
                finally:
                    CONNECTED.set(0)

                if stop.is_set():
                    break

                RECONNECTS.labels(reason=reason).inc()
                delay = min(backoff, BACKOFF_MAX) * (1 + random.random() * 0.3)
                log.warning("재연결 대기 %.1fs (사유=%s)", delay, reason)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, BACKOFF_MAX)
    finally:
        await producer.stop()
        log.info("종료 완료")


def cli() -> None:
    cfg = Config.from_env()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )
    asyncio.run(run(cfg))


if __name__ == "__main__":
    cli()
