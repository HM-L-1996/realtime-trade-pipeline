package dev.rtp.aggregator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeadLetterTest {

    /** 아무도 듣지 않는 주소. 적재는 반드시 실패한다. */
    private static final String UNREACHABLE = "http://127.0.0.1:1";

    @Test
    @DisplayName("페이로드가 길면 잘라낸다 - 형식이 바뀐 날 테이블이 폭발하지 않게")
    void 페이로드_절단() {
        assertEquals("", DeadLetter.truncate(null, 10));
        assertEquals("abc", DeadLetter.truncate("abc", 10));
        String cut = DeadLetter.truncate("x".repeat(50), 10);
        assertTrue(cut.startsWith("x".repeat(10)));
        assertTrue(cut.endsWith("(truncated)"));
    }

    // ---------------------------------------------------------------
    // 회귀 테스트 - 아래가 실제로 문제였다.
    // ---------------------------------------------------------------

    @Test
    @DisplayName("회귀: record() 는 절대 블로킹하지 않는다 - 처리 스레드를 붙잡으면 안 된다")
    void 논블로킹() throws Exception {
        // 이전 구현은 record() 안에서 곧바로 동기 HTTP 를 호출했다. 한 건에 한 번씩,
        // 최대 30초 타임아웃으로. 호출자는 Flink 처리 스레드다.
        //
        // 자체 실험에서 한 번에 4,255건을 버린 적이 있다. 그 조건이면 처리 스레드가
        // 수천 번의 순차 HTTP 호출에 묶인다 - "버려야 할 것" 때문에 본류가 멈춘다.
        //
        // 아래는 적재가 확실히 실패하는 주소로 3만 건을 밀어 넣는다.
        // 이전 구현이라면 끝나지 않는다.
        DeadLetter dl = new DeadLetter(UNREACHABLE, "u", "p", "test-run");
        try {
            long t0 = System.nanoTime();
            for (int i = 0; i < 30_000; i++) {
                dl.record("deserialize", "malformed-json", "005930", "payload-" + i);
            }
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

            assertTrue(elapsedMs < 15_000,
                    "record() 가 블로킹했다: " + elapsedMs + "ms 걸림");
        } finally {
            dl.close();
        }
    }

    @Test
    @DisplayName("회귀: 큐가 가득 차면 버리고 그 사실을 센다 - 메모리가 무한히 늘면 안 된다")
    void 큐_포화시_계측() throws Exception {
        // 큐를 무한히 두면 dead letter 를 지키려다 TaskManager 를 잃는다.
        // 버리되, 버렸다는 사실은 남아야 한다.
        DeadLetter dl = new DeadLetter(UNREACHABLE, "u", "p", "test-run");
        try {
            int n = DeadLetter.QUEUE_CAPACITY * 3;
            for (int i = 0; i < n; i++) {
                dl.record("window", "late-window", "005930", "p" + i);
            }
            assertTrue(dl.droppedByBackpressure() > 0,
                    "용량의 3배를 넣었는데 버린 건수가 0이다 - 큐가 무한하다는 뜻");
            assertTrue(dl.droppedByBackpressure() <= n,
                    "버린 건수가 넣은 건수보다 많다");
        } finally {
            dl.close();
        }
    }

    @Test
    @DisplayName("적재 실패는 예외로 올라오지 않는다 - dead letter 때문에 파이프라인이 서면 안 된다")
    void 실패를_삼킨다() throws Exception {
        DeadLetter dl = new DeadLetter(UNREACHABLE, "u", "p", "test-run");
        try {
            // 예외가 나면 이 지점에서 테스트가 깨진다.
            dl.record("deserialize", "missing-symbol", null, "{}");
            // 적재 스레드가 한 번은 시도할 시간을 준다.
            for (int i = 0; i < 40 && dl.failedWrites() == 0; i++) {
                Thread.sleep(100);
            }
            assertTrue(dl.failedWrites() > 0,
                    "적재가 실패했는데 failedWrites 가 0이다 - 실패가 계측되지 않는다");
            assertEquals(0, dl.writtenRows());
        } finally {
            dl.close();
        }
    }
}
