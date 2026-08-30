"""수집기 계측.

이 지표들의 존재 이유: 공식 캔들과 집계가 어긋났을 때
"내 파이프라인 버그"인지 "소스 프레임 유실"인지 구분하기 위해서다.
구분하지 못하면 validation.md 의 수치가 아무것도 증명하지 못한다.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

FRAMES = Counter(
    "rtp_ingester_frames_total",
    "수신한 WebSocket 프레임 수",
    ["frame_type"],
)
TRADES = Counter(
    "rtp_ingester_trades_total",
    "Kafka로 보낸 체결 수",
    ["symbol"],
)
PRODUCE_ERRORS = Counter(
    "rtp_ingester_produce_errors_total",
    "Kafka 전송 실패 수",
)
RECONNECTS = Counter(
    "rtp_ingester_reconnects_total",
    "WebSocket 재연결 횟수",
    ["reason"],
)
CONNECTED = Gauge(
    "rtp_ingester_connected",
    "WebSocket 연결 상태 (1=연결됨)",
)
# 소스 지연. event_time 과 수신 시각의 차이.
# 이 값이 튀는 구간이 프레임 유실이 일어나는 구간과 겹치는지 보기 위한 것.
SOURCE_LAG = Histogram(
    "rtp_ingester_source_lag_seconds",
    "체결 시각 대비 수신 지연",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)
# 역행한 타임스탬프. 소스가 순서를 보장하지 않는다는 증거가 되면 워터마크 전략이 바뀐다.
OUT_OF_ORDER = Counter(
    "rtp_ingester_out_of_order_total",
    "직전 체결보다 이른 event_time 이 온 횟수",
    ["symbol"],
)


def serve(port: int) -> None:
    start_http_server(port)
