package dev.rtp.ingester;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 멱등키 합성.
 *
 * <p>토스 체결 프레임에는 체결 ID 도 시퀀스 번호도 없다.
 * {@code (symbol, timestamp, price, volume)} 은 유일하지 않다 - 같은 밀리초에
 * 같은 가격·수량의 체결이 실제로 발생한다. 그래서 수집기가
 * {@code (symbol, eventMs)} 안의 일련번호를 부여한다.
 *
 * <h2>왜 밀리초별 카운터를 들고 있어야 하는가</h2>
 * 처음에는 "직전 이벤트타임" 하나만 기억하고 값이 바뀌면 0으로 리셋했다.
 * <b>그 구현은 순서 역전에서 키가 충돌한다.</b>
 *
 * <pre>
 *   1000 -> seq 0
 *   1000 -> seq 1
 *   1001 -> seq 0   (리셋)
 *   1000 -> seq 0   &lt;-- 첫 번째와 같은 키
 * </pre>
 *
 * 그리고 이 소스는 순서 역전이 <b>실제로</b> 일어난다 - 실측에서 005930 만
 * 2,195건이었다. 즉 이 충돌은 이론이 아니라 매일 발생하는 조건이다.
 * 멱등키를 만들려고 둔 장치가 멱등하지 않았다.
 *
 * <p>그래서 밀리초마다 카운터를 따로 들고 있는다. 무한히 쌓이지 않도록
 * {@link #HORIZON_MS} 보다 오래된 것은 버린다.
 *
 * <h2>지평을 워터마크 허용치보다 크게 잡아야 하는 이유</h2>
 * 지평 밖으로 밀려난 밀리초가 다시 등장하면 카운터가 0부터 시작해 또 충돌한다.
 * 그런 레코드는 워터마크를 한참 지나 도착한 것이라 집계 단계에서 어차피
 * 버려지지만, <b>두 값이 어긋나면 "버려지지 않으면서 키가 겹치는" 구간이 생긴다.</b>
 * 그래서 지평은 (워터마크 지연 + 허용 지연) 보다 넉넉히 커야 한다.
 * 현재 잡 기본값이 5초 + 0초이므로 2분이면 충분하다.
 *
 * <p>단일 수집 루프에서만 쓰인다. 스레드 안전하지 않다.
 */
public final class Sequencer {

    /**
     * 이 시간보다 오래된 밀리초의 카운터는 버린다.
     * 집계 잡의 워터마크 허용치보다 넉넉히 커야 한다 - 위 설명 참고.
     */
    static final long HORIZON_MS = 120_000L;

    /** {@link #assign} 의 결과. */
    public record Assignment(int seqInMs, boolean outOfOrder) {}

    private static final class State {
        /** eventMs -> 그 밀리초에서 이미 부여한 개수. 오래된 것은 잘라낸다. */
        final NavigableMap<Long, Integer> counts = new TreeMap<>();
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

        // merge 는 갱신 후 값을 돌려준다. 0-based 로 쓰려면 1을 뺀다.
        int seq = st.counts.merge(eventMs, 1, Integer::sum) - 1;

        boolean outOfOrder = st.maxEventMs != Long.MIN_VALUE && eventMs < st.maxEventMs;
        if (eventMs > st.maxEventMs) {
            st.maxEventMs = eventMs;
            // 지평 밖은 잘라낸다. TreeMap 이라 잘라내는 비용이 지운 개수에 비례한다.
            st.counts.headMap(st.maxEventMs - HORIZON_MS, false).clear();
        }
        return new Assignment(seq, outOfOrder);
    }

    /** 현재 들고 있는 밀리초 카운터 수. 메모리 증가를 확인하는 용도. */
    int trackedMillis(String symbol) {
        State st = states.get(symbol);
        return st == null ? 0 : st.counts.size();
    }
}
