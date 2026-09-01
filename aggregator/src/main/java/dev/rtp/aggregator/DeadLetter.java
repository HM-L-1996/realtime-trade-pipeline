package dev.rtp.aggregator;

import dev.rtp.model.ClickHouseHttp;

import java.io.Serializable;
import java.util.List;

/**
 * 버려진 레코드를 남긴다.
 *
 * <p>세기만 하고 버리면 "무엇이 왜 버려졌는지" 를 사후에 볼 수 없고, 고친 뒤
 * 재처리할 수도 없다. 카운터는 <b>몇 건인지</b>만 말해 준다 -
 * 특정 종목에 몰렸는지, 소스 형식이 바뀐 것인지, 우리 파서가 틀린 것인지는 답하지 못한다.
 *
 * <p>이 프로젝트의 원칙은 "조용히 사라지는 것은 반드시 계측한다" 인데,
 * 원본을 남기지 않으면 그 원칙을 절반만 지킨 것이다.
 *
 * <p><b>적재 실패를 삼킨다.</b> dead letter 를 쓰다 실패해서 파이프라인이 멈추면
 * 본말이 전도된다. 대신 실패도 카운터로 남긴다.
 */
public final class DeadLetter implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String TABLE = "rtp.dead_letters";
    public static final String COLUMNS = "stage, reason, symbol, payload, job_run";

    private final ClickHouseHttp ch;
    private final String jobRun;

    public DeadLetter(String baseUrl, String user, String password, String jobRun) {
        this.ch = new ClickHouseHttp(baseUrl, user, password);
        this.jobRun = jobRun;
    }

    /** @return 적재 성공 여부. 실패해도 예외를 던지지 않는다. */
    public boolean record(String stage, String reason, String symbol, String payload) {
        String row = "{\"stage\":" + ClickHouseHttp.quote(stage)
                + ",\"reason\":" + ClickHouseHttp.quote(reason)
                + ",\"symbol\":" + ClickHouseHttp.quote(symbol == null ? "" : symbol)
                // 페이로드가 클 수 있다. 원인 파악에는 앞부분이면 충분하고,
                // 통째로 넣으면 형식이 바뀐 날 테이블이 폭발한다.
                + ",\"payload\":" + ClickHouseHttp.quote(truncate(payload, 2000))
                + ",\"job_run\":" + ClickHouseHttp.quote(jobRun) + "}";
        try {
            ch.insertJsonEachRow(TABLE, COLUMNS, List.of(row));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "…(truncated)";
    }
}
