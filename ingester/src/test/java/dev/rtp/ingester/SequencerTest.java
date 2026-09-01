package dev.rtp.ingester;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequencerTest {

    /** 부여된 키를 모아 유일성을 검사하는 도우미. */
    private static String key(String symbol, long eventMs, Sequencer.Assignment a) {
        return symbol + '|' + eventMs + '|' + a.seqInMs();
    }

    @Test
    @DisplayName("같은 밀리초 체결은 일련번호가 증가한다 - 체결ID가 없으므로 이것이 유일한 구분 수단")
    void 같은_밀리초() {
        Sequencer s = new Sequencer();
        assertEquals(0, s.assign("005930", 1000).seqInMs());
        assertEquals(1, s.assign("005930", 1000).seqInMs());
        assertEquals(2, s.assign("005930", 1000).seqInMs());
    }

    @Test
    @DisplayName("종목별로 독립적이다")
    void 종목_독립() {
        Sequencer s = new Sequencer();
        s.assign("005930", 1000);
        s.assign("005930", 1000);
        assertEquals(0, s.assign("000660", 1000).seqInMs());
    }

    @Test
    @DisplayName("역행 타임스탬프를 감지한다 - 워터마크 지연 허용치의 근거가 된다")
    void 역행_감지() {
        Sequencer s = new Sequencer();
        assertFalse(s.assign("005930", 2000).outOfOrder());
        assertFalse(s.assign("005930", 2001).outOfOrder());
        assertTrue(s.assign("005930", 1999).outOfOrder());
        // 역행 후에도 최대값 기준은 유지된다
        assertTrue(s.assign("005930", 1998).outOfOrder());
    }

    @Test
    @DisplayName("첫 체결은 역행이 아니다")
    void 첫_체결() {
        Sequencer s = new Sequencer();
        assertFalse(s.assign("005930", 500).outOfOrder());
    }

    // ---------------------------------------------------------------
    // 회귀 테스트 - 아래가 실제로 터졌던 버그다.
    // ---------------------------------------------------------------

    @Test
    @DisplayName("회귀: 같은 밀리초가 비연속으로 재등장해도 키가 충돌하지 않는다")
    void 비연속_재등장_충돌() {
        // 이전 구현은 "직전 이벤트타임" 하나만 기억하고 값이 바뀌면 0으로 리셋했다.
        // 그래서 1000 -> 1000 -> 1001 -> 1000 순서에서 마지막이 seq 0 을 다시 받아
        // 첫 번째 레코드와 같은 키가 됐다.
        // 이 소스는 순서 역전이 실제로 일어난다(실측 005930 2,195건). 즉 매일 터질 조건이었다.
        Sequencer s = new Sequencer();
        Set<String> keys = new HashSet<>();

        for (long ms : new long[] {1000, 1000, 1001, 1000}) {
            assertTrue(keys.add(key("005930", ms, s.assign("005930", ms))),
                    "eventMs=" + ms + " 에서 멱등키가 충돌했다");
        }
        assertEquals(4, keys.size());
    }

    @Test
    @DisplayName("회귀: 심하게 뒤섞여 도착해도 모든 키가 유일하다")
    void 뒤섞인_도착_유일성() {
        Sequencer s = new Sequencer();
        Set<String> keys = new HashSet<>();
        // 세 밀리초를 번갈아, 각각 여러 번.
        long[] arrival = {1000, 1002, 1000, 1001, 1002, 1000, 1001, 1001, 1002, 1000};

        for (long ms : arrival) {
            assertTrue(keys.add(key("005930", ms, s.assign("005930", ms))),
                    "eventMs=" + ms + " 에서 멱등키가 충돌했다");
        }
        assertEquals(arrival.length, keys.size());
    }

    @Test
    @DisplayName("회귀: 같은 밀리초가 재등장하면 번호가 이어진다 (0 으로 돌아가지 않는다)")
    void 재등장시_번호_연속() {
        Sequencer s = new Sequencer();
        assertEquals(0, s.assign("005930", 1000).seqInMs());
        assertEquals(0, s.assign("005930", 1001).seqInMs());
        assertEquals(1, s.assign("005930", 1000).seqInMs(), "이어져야 한다");
        assertEquals(2, s.assign("005930", 1000).seqInMs());
    }

    @Test
    @DisplayName("지평 밖의 밀리초 카운터는 버려진다 - 메모리가 무한히 자라면 안 된다")
    void 지평_밖_정리() {
        Sequencer s = new Sequencer();
        long base = 1_000_000L;
        for (int i = 0; i < 500; i++) {
            s.assign("005930", base + i);
        }
        assertEquals(500, s.trackedMillis("005930"));

        // 지평을 훌쩍 넘긴 이벤트가 오면 이전 것들이 정리된다.
        s.assign("005930", base + Sequencer.HORIZON_MS + 10_000);
        assertTrue(s.trackedMillis("005930") < 500,
                "지평 밖 카운터가 정리되지 않았다: " + s.trackedMillis("005930"));
    }
}
