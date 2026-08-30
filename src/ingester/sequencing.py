"""멱등키 합성.

토스 체결 프레임에는 체결 ID도 시퀀스 번호도 없다.
(symbol, timestamp, price, volume) 은 유일하지 않다 —
같은 밀리초에 같은 가격·수량의 체결이 실제로 발생한다.

그래서 수집기가 (symbol, event_time) 안의 일련번호를 부여한다.
이 번호는 '수신 순서' 기준이므로 소스의 진짜 체결 순서와 다를 수 있다.
그래도 재처리 시 같은 입력에 같은 키가 나오는 성질은 유지된다 —
리플레이는 저장된 원본을 순서대로 다시 흘리기 때문이다.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SymbolState:
    last_event_ms: int = -1
    seq_in_ms: int = 0
    max_event_ms: int = -1


@dataclass
class Sequencer:
    _state: dict[str, SymbolState] = field(default_factory=dict)

    def assign(self, symbol: str, event_ms: int) -> tuple[int, bool]:
        """(seq_in_ms, out_of_order) 를 돌려준다.

        out_of_order 는 직전에 본 것보다 이른 event_time 이 왔다는 뜻이다.
        워터마크 지연 허용치를 정하는 근거가 된다.
        """
        st = self._state.get(symbol)
        if st is None:
            st = SymbolState()
            self._state[symbol] = st

        if event_ms == st.last_event_ms:
            st.seq_in_ms += 1
        else:
            st.last_event_ms = event_ms
            st.seq_in_ms = 0

        out_of_order = 0 <= st.max_event_ms and event_ms < st.max_event_ms
        if event_ms > st.max_event_ms:
            st.max_event_ms = event_ms

        return st.seq_in_ms, out_of_order
