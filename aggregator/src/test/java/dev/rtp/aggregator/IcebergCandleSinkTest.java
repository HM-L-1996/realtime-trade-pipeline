package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.Decimals;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IcebergCandleSinkTest {

    private static Candle sample() {
        return new Candle("005930",
                1_788_300_000_000L,
                Decimals.parsePrice("85000"),
                Decimals.parsePrice("85400.5"),
                Decimals.parsePrice("84900.25"),
                Decimals.parsePrice("85100"),
                Decimals.parseVolume("12345.00000001"),
                777L,
                "run-x");
    }

    @Test
    @DisplayName("가격·거래량이 Decimal 로 정확히 옮겨진다 - 근사되면 ClickHouse 와 대조가 무의미해진다")
    void 정밀도가_보존된다() {
        RowData r = IcebergCandleSink.toRowData(sample());

        assertEquals(new BigDecimal("85000.0000"),
                r.getDecimal(2, 18, Decimals.PRICE_SCALE).toBigDecimal());
        assertEquals(new BigDecimal("85400.5000"),
                r.getDecimal(3, 18, Decimals.PRICE_SCALE).toBigDecimal());
        assertEquals(new BigDecimal("84900.2500"),
                r.getDecimal(4, 18, Decimals.PRICE_SCALE).toBigDecimal());
        assertEquals(new BigDecimal("85100.0000"),
                r.getDecimal(5, 18, Decimals.PRICE_SCALE).toBigDecimal());

        // 소수 8자리 마지막 한 자리까지 살아 있어야 한다. double 을 거치면 여기서 깨진다.
        assertEquals(new BigDecimal("12345.00000001"),
                r.getDecimal(6, 18, Decimals.VOLUME_SCALE).toBigDecimal());
    }

    @Test
    @DisplayName("나머지 필드도 그대로 옮겨진다")
    void 필드_매핑() {
        RowData r = IcebergCandleSink.toRowData(sample());
        assertEquals("005930", r.getString(0).toString());
        assertEquals(1_788_300_000_000L, r.getTimestamp(1, 3).getMillisecond());
        assertEquals(777L, r.getLong(7));
        assertEquals("run-x", r.getString(8).toString());
    }

    @Test
    @DisplayName("runId 가 null 이어도 빈 문자열로 쓴다 - 스키마가 required 라 null 이면 쓰기가 실패한다")
    void runId_null() {
        Candle c = sample();
        c.runId = null;
        assertEquals("", IcebergCandleSink.toRowData(c).getString(8).toString());
    }

    @Test
    @DisplayName("스키마가 ClickHouse candles_1m 과 같은 정밀도를 쓴다")
    void 스키마_정밀도() {
        Types.DecimalType open =
                (Types.DecimalType) IcebergCandleSink.SCHEMA.findField("open").type();
        Types.DecimalType vol =
                (Types.DecimalType) IcebergCandleSink.SCHEMA.findField("volume").type();
        assertEquals(Decimals.PRICE_SCALE, open.scale());
        assertEquals(Decimals.VOLUME_SCALE, vol.scale());
        assertEquals(18, open.precision());
        assertEquals(18, vol.precision());
    }

    @Test
    @DisplayName("테이블 이름은 네임스페이스를 포함해야 한다")
    void 테이블_식별자() {
        JobConfig cfg = JobConfig.from(new String[] {"--iceberg-table", "rtp.candles_1m"});
        assertEquals("rtp", IcebergCandleSink.tableId(cfg).namespace().toString());
        assertEquals("candles_1m", IcebergCandleSink.tableId(cfg).name());
    }
}
