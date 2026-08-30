package dev.rtp.ingester;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rtp.model.TradeRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * 합성 체결 생성기.
 *
 * <p>존재 이유가 둘이다.
 * <ol>
 *   <li>국내장은 평일 09:00-15:30 만 열린다. 장외에 downstream 을 검증할 방법이 필요하다.
 *   <li>장애 실험에서 <b>지연 도착과 중복을 의도적으로 주입</b>해야 한다.
 *       자연 데이터를 기다려서는 원하는 상황이 언제 올지 알 수 없다.
 * </ol>
 *
 * <p>수집기와 <b>같은 {@link TradeRecord} 형식</b>으로 Kafka 에 넣는다. 형식이 갈리면
 * 이 도구로 검증한 것이 실제 경로를 보증하지 못한다. 그래서 두 쪽이 common 모듈의
 * 같은 타입을 쓴다.
 *
 * <pre>
 *   --rate 50 --duration 60
 *   --late-ratio 0.05 --late-max-s 120   지연 도착 주입
 *   --dup-ratio 0.02                     중복 주입
 *   --seed 42                            재현 가능한 실행
 * </pre>
 */
public final class SyntheticMain {

    private static final Logger log = LoggerFactory.getLogger(SyntheticMain.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);

        double rate = Double.parseDouble(a.getOrDefault("rate", "20"));
        double durationS = Double.parseDouble(a.getOrDefault("duration", "60"));
        double lateRatio = Double.parseDouble(a.getOrDefault("late-ratio", "0"));
        double lateMaxS = Double.parseDouble(a.getOrDefault("late-max-s", "90"));
        double dupRatio = Double.parseDouble(a.getOrDefault("dup-ratio", "0"));
        List<String> symbols = Config.symbols(a.getOrDefault("symbols", "005930,000660"));

        Random rnd = a.containsKey("seed")
                ? new Random(Long.parseLong(a.get("seed")))
                : new Random();

        // 자격 증명 없이 도는 도구다. Config.fromEnv() 를 쓰면 키를 요구하므로 직접 만든다.
        Config cfg = new Config("synthetic", "synthetic",
                "", "", symbols,
                envOr("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                envOr("KAFKA_TRADES_TOPIC", "trades.raw"),
                60_000L, 90_000L, 3_600_000L, 0);

        // 합성 데이터임이 ClickHouse 에서 구분되도록 connId 에 표시를 남긴다.
        String connId = "synth-" + UUID.randomUUID().toString().substring(0, 6);
        Sequencer sequencer = new Sequencer();
        Map<String, Double> price = new HashMap<>();
        for (int i = 0; i < symbols.size(); i++) {
            price.put(symbols.get(i), 70000.0 + 1000 * i);
        }

        long intervalNs = rate > 0 ? (long) (1_000_000_000L / rate) : 50_000_000L;
        long deadline = durationS > 0
                ? System.currentTimeMillis() + (long) (durationS * 1000)
                : Long.MAX_VALUE;

        long sent = 0;
        long late = 0;
        long dup = 0;
        long recvSeq = 0;

        log.info("시작 rate={}/s symbols={} connId={} late={}% dup={}%",
                rate, symbols, connId, lateRatio * 100, dupRatio * 100);

        try (Producer<String, byte[]> producer = IngesterMain.newProducer(cfg)) {
            while (System.currentTimeMillis() < deadline) {
                String sym = symbols.get(rnd.nextInt(symbols.size()));
                // 랜덤워크. 값 자체는 중요하지 않고 OHLCV 가 만들어지기만 하면 된다.
                price.compute(sym, (k, v) -> Math.max(1000.0, v + rnd.nextGaussian() * 30));

                long nowMs = System.currentTimeMillis();
                long eventMs = nowMs;
                if (rnd.nextDouble() < lateRatio) {
                    eventMs = nowMs - (long) ((1 + rnd.nextDouble() * (lateMaxS - 1)) * 1000);
                    late++;
                }

                recvSeq++;
                Sequencer.Assignment as = sequencer.assign(sym, eventMs);
                TradeRecord rec = new TradeRecord(
                        sym, eventMs, as.seqInMs(), nowMs,
                        String.format("%.2f", price.get(sym)),
                        String.valueOf(1 + rnd.nextInt(50)),
                        "KRW", connId, recvSeq);

                byte[] value = MAPPER.writeValueAsBytes(rec);
                producer.send(new ProducerRecord<>(cfg.topic(), sym, value));
                sent++;

                if (rnd.nextDouble() < dupRatio) {
                    // 완전히 동일한 레코드를 한 번 더. exactly-once 검증의 미끼.
                    producer.send(new ProducerRecord<>(cfg.topic(), sym, value));
                    dup++;
                }

                long until = System.nanoTime() + intervalNs;
                long sleepMs = (until - System.nanoTime()) / 1_000_000L;
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }
        }
        log.info("종료 sent={} late={} dup={}", sent, late, dup);
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String k = args[i];
            if (!k.startsWith("--")) {
                continue;
            }
            k = k.substring(2);
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                m.put(k, args[++i]);
            } else {
                m.put(k, "true");
            }
        }
        return m;
    }

    private static String envOr(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }

    private SyntheticMain() {}
}
