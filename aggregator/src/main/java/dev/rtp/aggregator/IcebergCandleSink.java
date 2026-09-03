package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.Decimals;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 캔들을 Iceberg 테이블에도 쓴다.
 *
 * <h2>왜 두 번째 싱크인가 - ClickHouse 를 대체하려는 것이 아니다</h2>
 * 두 싱크는 <b>전달 보장이 다르다.</b> 그 차이를 말로 적는 대신
 * <b>같은 파이프라인에서 나란히 돌려 결과로 보이려고</b> 붙였다.
 *
 * <ul>
 *   <li>ClickHouse - 배치가 차면 바로 HTTP 로 보낸다. <b>at-least-once</b>.
 *       재시작하면 같은 윈도가 다시 쓰일 수 있다.
 *   <li>Iceberg - 체크포인트가 완료될 때 커밋한다. <b>exactly-once</b>.
 *       커밋되지 않은 파일은 버려진다.
 * </ul>
 *
 * 이 저장소에는 잡을 재시작해 <b>같은 윈도가 420개 중복 기록된</b> 실측이 이미 있다.
 * 같은 조건에서 Iceberg 쪽은 중복이 생기지 않아야 한다 - 그것이 이 싱크의 존재 이유다.
 *
 * <h2>대가: 데이터가 보이기까지 지연이 생긴다</h2>
 * Iceberg 는 파일을 먼저 쓰고 <b>매니페스트에 커밋해야</b> 조회에 보인다.
 * Flink Iceberg 싱크는 그 커밋을 체크포인트 완료 시점에만 한다.
 * 그래서 <b>체크포인트 주기가 곧 조회 지연</b>이 된다 - 이 잡은 10초다.
 * 즉시성이 필요하면 ClickHouse 쪽을 봐야 한다. 공짜로 얻는 보장이 아니다.
 *
 * <h2>스키마를 ClickHouse 와 맞춘다</h2>
 * 두 저장소를 대조하려면 반올림 규칙이 같아야 하고, 그러려면 Decimal 정밀도가
 * 같아야 한다. float 로 두면 저장소 간 차이가 "검증 오차" 로 둔갑한다.
 */
public final class IcebergCandleSink {

    private static final Logger log = LoggerFactory.getLogger(IcebergCandleSink.class);

    /** ClickHouse 의 candles_1m 과 같은 정밀도. 대조를 위해 반드시 맞춘다. */
    private static final int PRECISION = 18;

    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "symbol", Types.StringType.get()),
            Types.NestedField.required(2, "window_start", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "open", Types.DecimalType.of(PRECISION, Decimals.PRICE_SCALE)),
            Types.NestedField.required(4, "high", Types.DecimalType.of(PRECISION, Decimals.PRICE_SCALE)),
            Types.NestedField.required(5, "low", Types.DecimalType.of(PRECISION, Decimals.PRICE_SCALE)),
            Types.NestedField.required(6, "close", Types.DecimalType.of(PRECISION, Decimals.PRICE_SCALE)),
            Types.NestedField.required(7, "volume", Types.DecimalType.of(PRECISION, Decimals.VOLUME_SCALE)),
            Types.NestedField.required(8, "trade_count", Types.LongType.get()),
            Types.NestedField.required(9, "run_id", Types.StringType.get()),
            Types.NestedField.required(10, "ingested_at", Types.TimestampType.withZone()));

    private IcebergCandleSink() {}

    /**
     * 테이블이 없으면 만든다. 잡이 시작할 때 한 번 한다.
     *
     * <p>배포 훅으로 빼는 방법도 있지만 그러면 스키마가 코드 밖에 있게 되어
     * 둘이 어긋날 여지가 생긴다. 스키마를 바꾸는 사람이 이 파일만 보면 되게 둔다.
     */
    static void ensureTable(JobConfig cfg) {
        try (RESTCatalog catalog = new RESTCatalog()) {
            catalog.initialize("rtp", catalogProps(cfg));
            TableIdentifier id = tableId(cfg);
            try {
                catalog.createNamespace(id.namespace());
                log.info("Iceberg 네임스페이스 생성 {}", id.namespace());
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
                // 이미 있으면 그대로 쓴다.
            }
            if (catalog.tableExists(id)) {
                log.info("Iceberg 테이블이 이미 있다 {}", id);
                return;
            }
            // 하루 단위 파티션. 조회가 대부분 날짜 범위로 들어오고,
            // 분 단위로 나누면 작은 파일이 폭증한다.
            PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("window_start").build();
            catalog.createTable(id, SCHEMA, spec, Map.of(
                    "write.format.default", "parquet",
                    "write.target-file-size-bytes", "134217728",
                    "format-version", "2"));
            log.info("Iceberg 테이블 생성 {} (파티션: day(window_start))", id);
        } catch (Exception e) {
            // 여기서는 예외를 올린다. ClickHouse dead letter 와 달리 이건 시작 시점
            // 문제라, 조용히 넘기면 "붙였는데 아무것도 안 쓰이는" 상태가 된다.
            // 이 저장소가 여러 번 당한 형태라 같은 실수를 반복하지 않는다.
            throw new IllegalStateException("Iceberg 테이블 준비 실패: " + e.getMessage(), e);
        }
    }

    /** 캔들 스트림에 Iceberg 싱크를 붙인다. */
    static void attach(DataStream<Candle> candles, JobConfig cfg) {
        // Iceberg 가 주는 TableLoader.fromCatalog 는 Hadoop Configuration 을 요구한다.
        // 이 이미지에도 iceberg-flink-runtime 에도 Hadoop 이 없고, 이 프로젝트는
        // Hadoop 을 끌어오지 않기로 이미 정해 두었다 - RestTableLoader 참고.
        TableLoader loader = new RestTableLoader(catalogProps(cfg), tableId(cfg));

        DataStream<RowData> rows = candles
                .map(IcebergCandleSink::toRowData)
                .name("candle-to-rowdata")
                .uid("candle-to-rowdata");

        FlinkSink.forRowData(rows)
                .tableLoader(loader)
                // 캔들은 초당 수 건이라 병렬로 쓸 이유가 없다. 1로 두면 체크포인트마다
                // 파티션당 파일이 하나씩만 생긴다 - 작은 파일 문제를 줄인다.
                .writeParallelism(1)
                // savepoint 로 상태를 넘기려면 연산자 uid 가 고정돼야 한다.
                // 이 잡의 다른 연산자들과 같은 이유로 접두어를 명시한다.
                .uidPrefix("iceberg-candles")
                .append();
    }

    static RowData toRowData(Candle c) {
        GenericRowData r = new GenericRowData(10);
        r.setField(0, StringData.fromString(c.symbol));
        r.setField(1, TimestampData.fromEpochMillis(c.windowStartMs));
        r.setField(2, dec(c.open, Decimals.PRICE_SCALE));
        r.setField(3, dec(c.high, Decimals.PRICE_SCALE));
        r.setField(4, dec(c.low, Decimals.PRICE_SCALE));
        r.setField(5, dec(c.close, Decimals.PRICE_SCALE));
        r.setField(6, dec(c.volume, Decimals.VOLUME_SCALE));
        r.setField(7, c.tradeCount);
        r.setField(8, StringData.fromString(c.runId == null ? "" : c.runId));
        r.setField(9, TimestampData.fromEpochMillis(System.currentTimeMillis()));
        return r;
    }

    /** 스케일된 long 을 그대로 Decimal 로 옮긴다. double 을 거치지 않는다. */
    private static DecimalData dec(long scaled, int scale) {
        return DecimalData.fromBigDecimal(BigDecimal.valueOf(scaled, scale), PRECISION, scale);
    }

    static TableIdentifier tableId(JobConfig cfg) {
        List<String> parts = List.of(cfg.icebergTable().split("\\."));
        if (parts.size() < 2) {
            throw new IllegalArgumentException(
                    "iceberg-table 은 네임스페이스.테이블 형식이어야 한다: " + cfg.icebergTable());
        }
        String[] ns = parts.subList(0, parts.size() - 1).toArray(new String[0]);
        return TableIdentifier.of(Namespace.of(ns), parts.get(parts.size() - 1));
    }

    /**
     * 카탈로그 접속 설정.
     *
     * <p>자격증명은 여기 쓰지 않는다. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY 를
     * Secret 에서 환경변수로 주입하고 S3FileIO 가 기본 자격증명 체인에서 읽는다.
     * flinkConfiguration 이나 잡 인자에 넣으면
     * kubectl get flinkdeployment -o yaml 로 평문이 그대로 보인다.
     */
    private static Map<String, String> catalogProps(JobConfig cfg) {
        Map<String, String> p = new HashMap<>();
        p.put("type", "rest");
        p.put("uri", cfg.icebergCatalogUri());
        p.put("warehouse", cfg.icebergWarehouse());
        p.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        p.put("s3.endpoint", cfg.s3Endpoint());
        p.put("s3.path-style-access", "true");
        return p;
    }
}
