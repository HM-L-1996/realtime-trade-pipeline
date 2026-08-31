package dev.rtp.ingester;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rtp.model.TradeRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * 저장된 체결을 다시 흘린다.
 *
 * <h2>왜 필요한가</h2>
 * <ol>
 *   <li><b>장외에 실험할 수 없다.</b> 시장은 평일 08:00-20:00 에만 흐른다.
 *       나머지 시간에 워터마크·체크포인트·백프레셔 실험을 하려면 데이터를 우리가 만들어야 한다.
 *   <li><b>실험은 재현 가능해야 한다.</b> "워터마크 지연 5초 vs 30초" 를 비교하려면
 *       <b>같은 입력</b>을 두 번 흘려야 한다. 라이브 데이터로는 같은 입력이 두 번 오지 않는다.
 *   <li>지연 도착·순서 역전을 자연 데이터로 기다릴 수 없다.
 * </ol>
 *
 * <p>합성 생성기({@link SyntheticMain})와 다른 점은 <b>실제 시장 데이터의 분포</b>를
 * 그대로 쓴다는 것이다. 체결 간격, 가격 변동, 종목별 거래량 편차가 진짜다.
 *
 * <h2>소스</h2>
 * ClickHouse {@code trades_raw} 가 아니라 Kafka 를 읽는다. 그 테이블을 채우는 주체가
 * 아직 없고, Kafka 보존이 7일이라 같은 구간을 이미 갖고 있기 때문이다.
 *
 * <h2>이벤트타임 재기준</h2>
 * 원본 이벤트타임을 그대로 쓰면 전부 과거라 워터마크가 즉시 튀어 모든 윈도가 한꺼번에
 * 닫힌다. 그러면 "지연 도착이 윈도에 미치는 영향" 을 볼 수 없다.
 * {@code --shift-to-now} 로 시간축을 현재로 옮기고, 원본의 <b>상대 간격</b>을 유지하며 흘린다.
 *
 * <pre>
 *   java -cp ingester.jar dev.rtp.ingester.ReplayMain \
 *        --source-topic trades.raw --target-topic trades.replay \
 *        --speed 1.0 --shift-to-now --late-ratio 0.05 --seed 42
 * </pre>
 */
public final class ReplayMain {

    private static final Logger log = LoggerFactory.getLogger(ReplayMain.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Map<String, String> a = SyntheticMain.parseArgs(args);

        String sourceTopic = a.getOrDefault("source-topic", "trades.raw");
        String targetTopic = a.getOrDefault("target-topic", "trades.replay");
        double speed = Double.parseDouble(a.getOrDefault("speed", "1.0"));
        int maxRecords = Integer.parseInt(a.getOrDefault("max-records", "200000"));
        boolean shiftToNow = a.containsKey("shift-to-now");
        double lateRatio = Double.parseDouble(a.getOrDefault("late-ratio", "0"));
        double lateMaxS = Double.parseDouble(a.getOrDefault("late-max-s", "90"));
        double dupRatio = Double.parseDouble(a.getOrDefault("dup-ratio", "0"));

        Random rnd = a.containsKey("seed")
                ? new Random(Long.parseLong(a.get("seed")))
                : new Random();

        // 특정 연결의 레코드만 재생한다.
        // 토픽 앞부분에 합성 데이터가 섞여 있으면 이벤트타임에 큰 구멍이 생기고,
        // 워터마크가 그 지점에서 껑충 뛰어 지연 주입 효과가 묻힌다.
        // 설정 비교 실험은 **연속된 실제 구간**에서만 의미가 있다.
        String connFilter = a.getOrDefault("source-conn-id", "");
        // 앞에서부터 건너뛸 개수. 구간을 뒤로 옮길 때 쓴다.
        int skip = Integer.parseInt(a.getOrDefault("skip-records", "0"));

        String bootstrap = envOr("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        List<TradeRecord> loaded = load(bootstrap, sourceTopic, maxRecords, connFilter, skip);
        if (loaded.isEmpty()) {
            log.warn("{} 에서 읽은 레코드가 없다. 재생할 것이 없다.", sourceTopic);
            return;
        }

        // 원본 수신 순서로 정렬한다. 이것이 실제로 데이터가 도착했던 순서다 -
        // 이벤트타임으로 정렬해 버리면 원본에 있던 순서 역전이 사라져,
        // "순서 역전이 집계에 미치는 영향" 을 재현할 수 없게 된다.
        loaded.sort(Comparator.comparingLong(TradeRecord::ingestMs));

        // 원본에 긴 공백이 있으면(수집기를 껐다 켠 구간, 장 마감~다음 개장) 그것까지
        // 그대로 재생하게 되어 몇 시간을 기다려야 한다. 실험 도구로는 쓸 수 없다.
        // maxGapMs 를 넘는 간격은 그 값으로 압축한다.
        long maxGapMs = Long.parseLong(a.getOrDefault("max-gap-seconds", "5")) * 1000L;
        long[] schedule = compressGaps(loaded, maxGapMs);

        long firstIngest = loaded.get(0).ingestMs();
        long lastIngest = loaded.get(loaded.size() - 1).ingestMs();
        long minEvent = loaded.stream().mapToLong(TradeRecord::eventMs).min().orElse(firstIngest);

        long startWall = System.currentTimeMillis();
        // 시간축을 현재로 옮긴다. 가장 이른 이벤트가 "지금" 이 되도록.
        long shift = shiftToNow ? (startWall - minEvent) : 0L;

        String connId = "replay-" + UUID.randomUUID().toString().substring(0, 6);
        log.info("리플레이 시작 {} -> {} 레코드={} 원본구간={}초 압축후={}초 speed={} connId={}",
                sourceTopic, targetTopic, loaded.size(),
                (lastIngest - firstIngest) / 1000,
                (long) (schedule[schedule.length - 1] / speed / 1000), speed, connId);

        Config cfg = new Config("replay", "replay", "", "", List.of(),
                bootstrap, targetTopic, 60_000L, 90_000L, 3_600_000L, 0);

        long sent = 0;
        long late = 0;
        long dup = 0;
        long recvSeq = 0;

        try (Producer<String, byte[]> producer = IngesterMain.newProducer(cfg)) {
            for (int i = 0; i < loaded.size(); i++) {
                TradeRecord t = loaded.get(i);
                // 원본의 상대 간격을 유지한다(긴 공백은 압축됨). speed 로 배속.
                long targetOffsetMs = (long) (schedule[i] / speed);
                long waitMs = (startWall + targetOffsetMs) - System.currentTimeMillis();
                if (waitMs > 0) {
                    Thread.sleep(waitMs);
                }

                long nowMs = System.currentTimeMillis();
                long eventMs = t.eventMs() + shift;
                if (rnd.nextDouble() < lateRatio) {
                    eventMs -= (long) ((1 + rnd.nextDouble() * (lateMaxS - 1)) * 1000);
                    late++;
                }

                recvSeq++;
                TradeRecord out = new TradeRecord(
                        t.symbol(), eventMs, t.seqInMs(), nowMs,
                        t.price(), t.volume(), t.currency(), connId, recvSeq);

                byte[] value = MAPPER.writeValueAsBytes(out);
                producer.send(new ProducerRecord<>(targetTopic, t.symbol(), value));
                sent++;

                if (rnd.nextDouble() < dupRatio) {
                    producer.send(new ProducerRecord<>(targetTopic, t.symbol(), value));
                    dup++;
                }
            }
        }
        log.info("리플레이 종료 sent={} late={} dup={} 소요={}초",
                sent, late, dup, (System.currentTimeMillis() - startWall) / 1000);
    }

    /**
     * 각 레코드의 재생 시각(첫 레코드 기준 오프셋 ms)을 만든다.
     *
     * <p>원본의 간격을 그대로 쓰되 {@code maxGapMs} 를 넘는 공백은 그 값으로 줄인다.
     * 수집기를 껐다 켠 구간이나 장 마감~개장 사이를 실시간으로 기다릴 이유가 없다.
     * 공백 안쪽의 조밀한 구간은 원본 간격이 유지되므로 버스트 특성은 보존된다.
     */
    static long[] compressGaps(List<TradeRecord> sorted, long maxGapMs) {
        long[] out = new long[sorted.size()];
        long acc = 0;
        for (int i = 1; i < sorted.size(); i++) {
            long gap = sorted.get(i).ingestMs() - sorted.get(i - 1).ingestMs();
            if (gap < 0) {
                gap = 0;
            }
            acc += Math.min(gap, maxGapMs);
            out[i] = acc;
        }
        return out;
    }

    /** 소스 토픽을 처음부터 읽어 메모리에 담는다. connFilter 가 있으면 그 연결만 남긴다. */
    private static List<TradeRecord> load(String bootstrap, String topic, int max,
                                          String connFilter, int skip) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // 재생용 일회성 소비다. 오프셋을 커밋하지 않는다 - 커밋하면 다음 재생이
        // 이어서 읽게 되어 "같은 입력을 두 번" 이라는 전제가 깨진다.
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "replay-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<TradeRecord> out = new ArrayList<>();
        int malformed = 0;
        int skipped = 0;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(Collections.singletonList(topic));
            int emptyPolls = 0;
            while (out.size() < max && emptyPolls < 3) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, byte[]> r : records) {
                    if (r.value() == null) {
                        continue;
                    }
                    try {
                        TradeRecord t = MAPPER.readValue(r.value(), TradeRecord.class);
                        if (!connFilter.isEmpty() && !connFilter.equals(t.connId())) {
                            continue;
                        }
                        if (skipped < skip) {
                            skipped++;
                            continue;
                        }
                        out.add(t);
                    } catch (Exception e) {
                        malformed++;
                    }
                    if (out.size() >= max) {
                        break;
                    }
                }
            }
        }
        if (malformed > 0) {
            log.warn("역직렬화 실패 {}건은 건너뛰었다", malformed);
        }
        log.info("{} 에서 {}건 로드", topic, out.size());
        return out;
    }

    private static String envOr(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }

    private ReplayMain() {}
}
