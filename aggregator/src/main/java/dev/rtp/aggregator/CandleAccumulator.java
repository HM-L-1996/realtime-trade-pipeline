package dev.rtp.aggregator;

/**
 * 분봉 누산기.
 *
 * <p>Flink POJO 규약(public 필드 + 인자 없는 생성자)을 따른다. 누산기는 체크포인트에
 * 저장되는 상태이므로 Kryo 로 떨어지면 savepoint 스키마 진화가 막힌다.
 *
 * <p><b>시가·종가를 도착 순서로 잡지 않는 것이 핵심이다.</b> 지연 도착이 있으면
 * 나중에 도착한 체결이 더 이른 이벤트타임일 수 있다. 도착 순서로 open 을 정하면
 * 늦게 온 진짜 첫 체결이 무시되어 공식 캔들과 어긋난다 - 그리고 그 차이는
 * "워터마크 설정 탓" 으로 오인되기 딱 좋다.
 *
 * <p>동률(같은 밀리초)은 수집기가 매긴 {@code seqInMs} 로 가른다. 체결 ID 가 없는
 * 소스라 이것이 유일한 2차 기준이다.
 */
public class CandleAccumulator {

    public String symbol;

    public long openEventMs = Long.MAX_VALUE;
    public int openSeq = Integer.MAX_VALUE;
    public long open;

    public long closeEventMs = Long.MIN_VALUE;
    public int closeSeq = Integer.MIN_VALUE;
    public long close;

    public long high = Long.MIN_VALUE;
    public long low = Long.MAX_VALUE;

    public long volume;
    public long tradeCount;

    public CandleAccumulator() {}

    public boolean isEmpty() {
        return tradeCount == 0;
    }

    /** 이벤트타임이 더 이르면(동률이면 seq 가 작으면) 시가를 갈아끼운다. */
    public void offerOpen(long eventMs, int seq, long price) {
        if (eventMs < openEventMs || (eventMs == openEventMs && seq < openSeq)) {
            openEventMs = eventMs;
            openSeq = seq;
            open = price;
        }
    }

    /** 이벤트타임이 더 늦으면(동률이면 seq 가 크면) 종가를 갈아끼운다. */
    public void offerClose(long eventMs, int seq, long price) {
        if (eventMs > closeEventMs || (eventMs == closeEventMs && seq > closeSeq)) {
            closeEventMs = eventMs;
            closeSeq = seq;
            close = price;
        }
    }
}
