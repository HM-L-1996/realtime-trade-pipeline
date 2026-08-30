"""토스 WebSocket 체결 스트림 클라이언트.

문서에서 확인한 제약이 그대로 구현 제약이 된다.

- 구독은 **선언형 전체 교체**다. 배열 하나가 현재 구독 집합 전체이고,
  빠진 항목은 자동 해제된다. 증분 추가로 착각하면 조용히 구독이 날아간다.
- keepalive 는 JSON 이 아니라 순수 텍스트 "PING" 이다.
- 스트림이 lossy 하다. 시퀀스 번호가 없어 유실을 스트림만으로 탐지할 수 없다.
  → 연결별 recv_seq 를 우리가 매겨서 최소한 '어느 연결에서 몇 번째였는지'는 남긴다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from .metrics import FRAMES

log = logging.getLogger(__name__)

PING_TEXT = "PING"  # 대문자 순수 텍스트. JSON 이 아니다.


class TossTradeStream:
    def __init__(self, ws_url: str, symbols: list[str],
                 ping_interval_s: float, recv_timeout_s: float) -> None:
        self._url = ws_url
        self._symbols = symbols
        self._ping_interval = ping_interval_s
        self._recv_timeout = recv_timeout_s

    def _subscription(self) -> str:
        # 전체 교체 선언. 종목을 추가할 때도 기존 목록을 전부 다시 보내야 한다.
        return json.dumps(
            [
                {"id": f"sub-{uuid.uuid4().hex[:8]}"},
                {"type": "trade:kr", "codes": self._symbols},
            ],
            ensure_ascii=False,
        )

    async def stream(self, token: str) -> AsyncIterator[dict[str, Any]]:
        """한 번의 연결을 유지하며 체결 프레임을 흘린다.

        연결이 끊기면 예외가 올라간다. 재연결·백오프는 호출자가 담당한다.
        """
        conn_id = uuid.uuid4().hex[:12]
        headers = {"Authorization": f"Bearer {token}"}

        async with websockets.connect(
            self._url,
            additional_headers=headers,
            open_timeout=15,
            close_timeout=5,
            # 라이브러리 자체 ping 은 끈다. 서버가 요구하는 형식이 텍스트 "PING" 이다.
            ping_interval=None,
        ) as ws:
            await ws.send(self._subscription())
            log.info("구독 선언 전송 conn_id=%s symbols=%s", conn_id, self._symbols)

            pinger = asyncio.create_task(self._ping_loop(ws))
            recv_seq = 0
            try:
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self._recv_timeout)
                    except asyncio.TimeoutError:
                        # 서버가 180초에 끊는다. 그때까지 기다리면 유실 구간이 길어진다.
                        raise ConnectionClosed(None, None) from None

                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", "replace")

                    frame = self._parse(raw)
                    if frame is None:
                        continue

                    ftype = frame.get("type", "unknown")
                    FRAMES.labels(frame_type=ftype).inc()

                    if ftype == "message":
                        recv_seq += 1
                        parsed = self._to_trade(frame, conn_id, recv_seq)
                        if parsed is not None:
                            yield parsed
                    elif ftype == "subscriptions":
                        rejected = frame.get("rejected") or []
                        if rejected:
                            log.error("구독 거부됨: %s", rejected)
                        else:
                            log.info("구독 확인됨 conn_id=%s", conn_id)
                    elif ftype == "error":
                        log.error("서버 오류 프레임: %s", frame)
                        if frame.get("code") == "server-shutdown":
                            raise ConnectionClosed(None, None)
                    elif ftype == "pong":
                        pass
                    else:
                        log.debug("알 수 없는 프레임 type=%s", ftype)
            finally:
                pinger.cancel()

    async def _ping_loop(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(self._ping_interval)
                await ws.send(PING_TEXT)
        except (asyncio.CancelledError, ConnectionClosed):
            pass

    @staticmethod
    def _parse(raw: str) -> dict[str, Any] | None:
        s = raw.strip()
        if not s or s == "PONG":
            return None
        try:
            obj = json.loads(s)
        except json.JSONDecodeError:
            log.warning("JSON 파싱 실패: %.120s", s)
            return None
        return obj if isinstance(obj, dict) else None

    @staticmethod
    def _to_trade(frame: dict[str, Any], conn_id: str, recv_seq: int) -> dict[str, Any] | None:
        topic = frame.get("topic", "")
        data = frame.get("data")
        if not isinstance(data, dict):
            return None
        # topic 형식: "trade:kr:005930"
        parts = topic.split(":")
        if len(parts) != 3 or parts[0] != "trade":
            return None
        symbol = parts[2]

        ts = data.get("timestamp")
        if not ts:
            return None
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            log.warning("timestamp 파싱 실패: %s", ts)
            return None
        if dt.tzinfo is None:
            # 문서상 +09:00 이 붙어 오지만, 없으면 KST 로 가정하지 않고 버린다.
            log.warning("timezone 없는 timestamp: %s", ts)
            return None
        event_ms = int(dt.astimezone(timezone.utc).timestamp() * 1000)

        return {
            "symbol": symbol,
            "event_ms": event_ms,
            # 가격·수량은 문자열 그대로 넘긴다. float 로 바꾸는 순간
            # 부동소수 오차가 검증 오차로 둔갑한다.
            "price": str(data.get("price", "")),
            "volume": str(data.get("volume", "")),
            "currency": data.get("currency", ""),
            "conn_id": conn_id,
            "recv_seq": recv_seq,
        }
