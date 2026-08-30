package dev.rtp.ingester;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequencerTest {

    @Test
    @DisplayName("같은 밀리초 체결은 일련번호가 증가한다 - 체결ID가 없으므로 이것이 유일한 구분 수단")
    void 같은_밀리초() {
        Sequencer s = new Sequencer();
        assertEquals(0, s.assign("005930", 1000).seqInMs());
        assertEquals(1, s.assign("005930", 1000).seqInMs());
        assertEquals(2, s.assign("005930", 1000).seqInMs());
    }

    @Test
    @DisplayName("밀리초가 바뀌면 일련번호가 초기화된다")
    void 밀리초_변경() {
        Sequencer s = new Sequencer();
        s.assign("005930", 1000);
        s.assign("005930", 1000);
        assertEquals(0, s.assign("005930", 1001).seqInMs());
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
}
