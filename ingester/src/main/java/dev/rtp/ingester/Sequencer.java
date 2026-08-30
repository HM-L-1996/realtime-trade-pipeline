package dev.rtp.ingester;

import java.util.HashMap;
import java.util.Map;

/**
 * 멱등키 합성.
 *
 * <p>토스 체결 프레임에는 체결 ID 도 시퀀스 번호도 없다.
 * {@code (symbol, timestamp, price, volume)} 은 유일하지 않다 - 같은 밀리초에
 * 같은 가격·수량의 체결이 실제로 발생한다. 그래서 수집기가
 * {@code (symbol, eventMs)} 안의 일련번호를 부여한다.
 *
 * <p>이 번호는 <b>수신 순서</b> 기준이므로 소스의 진짜 체결 순서와 다를 수 있다.
 * 그래도 재처리 시 같은 입력에 같은 키가 나오는 성질은 유지된다 -
 * 리플레이는 저장된 원본을 순서대로 다시 흘리기 때문이다.
 *
 * <p>단일 수집 루프에서만 쓰인다. 스레드 안전하지 않다.
 */
public final class Sequencer {

    /** {@link #assign} 의 결과. */
    public record Assignment(int seqInMs, boolean outOfOrder) {}

    private static final class State {
        long lastEventMs = -1;
        int seqInMs = 0;
        long maxEventMs = Long.MIN_VALUE;
    }

    private final Map<String, State> states = new HashMap<>();

    /**
     * 일련번호를 부여하고, 직전에 본 것보다 이른 이벤트타임인지 알려준다.
     *
     * <p>{@code outOfOrder} 는 소스가 순서를 보장하지 않는다는 증거다.
     * 워터마크 지연 허용치를 정하는 근거가 된다.
     */
    public Assignment assign(String symbol, long eventMs) {
        State st = states.computeIfAbsent(symbol, k -> new State());

        if (eventMs == st.lastEventMs) {
            st.seqInMs++;
        } else {
            st.lastEventMs = eventMs;
            st.seqInMs = 0;
        }

        boolean outOfOrder = st.maxEventMs != Long.MIN_VALUE && eventMs < st.maxEventMs;
        if (eventMs > st.maxEventMs) {
            st.maxEventMs = eventMs;
        }
        return new Assignment(st.seqInMs, outOfOrder);
    }
}
