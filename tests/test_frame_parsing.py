import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from ingester.toss_ws import TossTradeStream

P = TossTradeStream._to_trade


def _frame(**data):
    base = {"price": "71800", "volume": "12",
            "timestamp": "2026-06-18T09:30:00.123+09:00", "currency": "KRW"}
    base.update(data)
    return {"type": "message", "topic": "trade:kr:005930", "data": base}


def test_체결_프레임을_파싱한다():
    t = P(_frame(), "conn1", 7)
    assert t is not None
    assert t["symbol"] == "005930"
    assert t["conn_id"] == "conn1"
    assert t["recv_seq"] == 7
    assert t["currency"] == "KRW"


def test_KST를_UTC_밀리초로_정규화한다():
    """09:30:00.123+09:00 == 00:30:00.123Z"""
    t = P(_frame(), "c", 1)
    from datetime import datetime, timezone
    expect = int(datetime(2026, 6, 18, 0, 30, 0, 123000, tzinfo=timezone.utc).timestamp() * 1000)
    assert t["event_ms"] == expect


def test_가격과_수량을_문자열로_보존한다():
    """float 로 바꾸면 부동소수 오차가 검증 오차로 둔갑한다."""
    t = P(_frame(price="0.1", volume="0.3"), "c", 1)
    assert t["price"] == "0.1"
    assert t["volume"] == "0.3"
    assert isinstance(t["price"], str)


def test_timezone_없는_timestamp는_버린다():
    """KST 로 임의 가정하면 이벤트타임이 9시간 틀어진다."""
    assert P(_frame(timestamp="2026-06-18T09:30:00.123"), "c", 1) is None


def test_잘못된_timestamp는_버린다():
    assert P(_frame(timestamp="not-a-time"), "c", 1) is None
    assert P(_frame(timestamp=""), "c", 1) is None


def test_체결이_아닌_토픽은_무시한다():
    f = _frame()
    f["topic"] = "orderbook:kr:005930"
    assert P(f, "c", 1) is None


def test_data가_없으면_무시한다():
    assert P({"type": "message", "topic": "trade:kr:005930"}, "c", 1) is None


def test_미국_체결도_파싱된다():
    f = _frame(currency="USD")
    f["topic"] = "trade:us:AAPL"
    t = P(f, "c", 1)
    assert t["symbol"] == "AAPL"
