"""합성 체결 생성기.

존재 이유가 둘이다.

1. 국내장은 평일 09:00-15:30 만 열린다. 장외에 downstream 을 검증할 방법이 필요하다.
2. 장애 실험에서 **지연 도착·순서 역전·중복을 의도적으로 주입**해야 한다.
   자연 데이터를 기다려서는 원하는 상황이 언제 올지 알 수 없다.

수집기(main.py)와 **완전히 같은 레코드 형식**으로 Kafka 에 넣는다.
형식이 갈리면 이 도구로 검증한 것이 실제 경로를 보증하지 못한다.

사용:
  python -m ingester.synthetic --rate 50 --duration 60
  python -m ingester.synthetic --late-ratio 0.05 --late-max-s 120   # 지연 도착 주입
  python -m ingester.synthetic --dup-ratio 0.02                      # 중복 주입
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import time
import uuid

from aiokafka import AIOKafkaProducer

from .config import Config
from .sequencing import Sequencer

log = logging.getLogger("synthetic")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="합성 체결을 Kafka 로 흘린다")
    p.add_argument("--rate", type=float, default=20.0, help="초당 체결 수")
    p.add_argument("--duration", type=float, default=60.0, help="실행 시간(초). 0이면 무한")
    p.add_argument("--symbols", default="005930,000660")
    p.add_argument("--late-ratio", type=float, default=0.0,
                   help="지연 도착으로 만들 비율 (0~1)")
    p.add_argument("--late-max-s", type=float, default=90.0,
                   help="지연 도착의 최대 과거 초")
    p.add_argument("--dup-ratio", type=float, default=0.0,
                   help="같은 체결을 한 번 더 보낼 비율 (0~1)")
    p.add_argument("--seed", type=int, default=None, help="재현 가능한 실행용")
    return p.parse_args()


async def main() -> None:
    a = _args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)-5s %(message)s")
    if a.seed is not None:
        random.seed(a.seed)

    cfg = Config(client_id="synthetic", client_secret="synthetic")
    symbols = [s.strip() for s in a.symbols.split(",") if s.strip()]
    # 합성 데이터임이 ClickHouse 에서 구분되도록 conn_id 에 표시를 남긴다.
    conn_id = "synth-" + uuid.uuid4().hex[:6]
    seq = Sequencer()
    price = {s: 70000.0 + 1000 * i for i, s in enumerate(symbols)}

    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.bootstrap_servers,
        acks="all", enable_idempotence=True, linger_ms=20,
    )
    await producer.start()
    log.info("시작 rate=%.1f/s symbols=%s conn_id=%s late=%.0f%% dup=%.0f%%",
             a.rate, symbols, conn_id, a.late_ratio * 100, a.dup_ratio * 100)

    interval = 1.0 / a.rate if a.rate > 0 else 0.05
    deadline = time.time() + a.duration if a.duration > 0 else float("inf")
    recv_seq = 0
    sent = 0
    late_sent = 0
    dup_sent = 0

    try:
        while time.time() < deadline:
            sym = random.choice(symbols)
            # 랜덤워크. 값 자체는 중요하지 않고 OHLCV 가 만들어지기만 하면 된다.
            price[sym] = max(1000.0, price[sym] + random.gauss(0, 30))

            now_ms = int(time.time() * 1000)
            event_ms = now_ms
            is_late = random.random() < a.late_ratio
            if is_late:
                event_ms = now_ms - int(random.uniform(1, a.late_max_s) * 1000)
                late_sent += 1

            recv_seq += 1
            s_in_ms, _ = seq.assign(sym, event_ms)
            rec = {
                "symbol": sym,
                "event_ms": event_ms,
                "seq_in_ms": s_in_ms,
                "ingest_ms": now_ms,
                "price": f"{price[sym]:.2f}",
                "volume": str(random.randint(1, 50)),
                "currency": "KRW",
                "conn_id": conn_id,
                "recv_seq": recv_seq,
            }
            payload = json.dumps(rec, ensure_ascii=False, separators=(",", ":")).encode()
            await producer.send_and_wait(cfg.topic, key=sym.encode(), value=payload)
            sent += 1

            if random.random() < a.dup_ratio:
                # 완전히 동일한 레코드를 한 번 더. exactly-once 검증의 미끼.
                await producer.send_and_wait(cfg.topic, key=sym.encode(), value=payload)
                dup_sent += 1

            await asyncio.sleep(interval)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await producer.stop()
        log.info("종료 sent=%d late=%d dup=%d", sent, late_sent, dup_sent)


if __name__ == "__main__":
    asyncio.run(main())
