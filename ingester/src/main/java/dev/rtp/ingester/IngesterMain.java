package dev.rtp.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rtp.model.TradeRecord;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 토스 WebSocket 체결 -&gt; Kafka.
 *
 * <p>재연결, 백오프, 토큰 갱신을 여기서 묶는다. 소스가 lossy 하므로 끊긴 구간을
 * connId 로 남겨 나중에 유실을 추적할 수 있게 한다.
 */
public final class IngesterMain {

    private static final Logger log = LoggerFactory.getLogger(IngesterMain.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final long BACKOFF_START_MS = 1_000L;
    private static final long BACKOFF_MAX_MS = 30_000L;

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        new IngesterMain(cfg).run();
    }

    private final Config cfg;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final Random random = new Random();

    IngesterMain(Config cfg) {
        this.cfg = cfg;
    }

    void run() throws Exception {
        try (HTTPServer ignored = Metrics.serve(cfg.metricsPort());
             Producer<String, byte[]> producer = newProducer(cfg)) {

            log.info("수집기 시작 {}", cfg.redacted());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("종료 신호 수신");
                stop.set(true);
            }));

            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            TokenProvider tokens = new TokenProvider(cfg, http);
            Sequencer sequencer = new Sequencer();

            long backoff = BACKOFF_START_MS;
            while (!stop.get()) {
                String reason;
                try {
                    // 재연결 시점에 토큰이 만료돼 있으면 여기서 걸린다.
                    // WebSocket 은 핸드셰이크에서만 토큰을 보므로 이 갱신이 유일한 방어선이다.
                    String token = tokens.get();
                    boolean progressed = pump(http, token, producer, sequencer);
                    reason = "stream-ended";
                    if (progressed) {
                        backoff = BACKOFF_START_MS;
                    }
                } catch (TokenProvider.TokenException e) {
                    reason = "token";
                    log.error("토큰 오류: {}", e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    reason = "error-" + e.getClass().getSimpleName();
                    log.error("스트림 예외", e);
                } finally {
                    Metrics.CONNECTED.set(0);
                }

                if (stop.get()) {
                    break;
                }
                Metrics.RECONNECTS.labels(reason).inc();
                long delay = (long) (Math.min(backoff, BACKOFF_MAX_MS) * (1 + random.nextDouble() * 0.3));
                log.warn("재연결 대기 {}ms (사유={})", delay, reason);
                Thread.sleep(delay);
                backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
            }
            log.info("종료 완료");
        }
    }

    /**
     * 한 연결을 끝까지 소비한다.
     *
     * @return 체결을 하나라도 받았으면 true (백오프 초기화 판단에 쓴다)
     */
    private boolean pump(HttpClient http, String token, Producer<String, byte[]> producer,
                         Sequencer sequencer) throws Exception {
        boolean progressed = false;
        try (TossTradeStream stream = new TossTradeStream(cfg)) {
            stream.connect(http, token);
            Metrics.CONNECTED.set(1);

            long recvSeq = 0;
            long nextPingAt = System.currentTimeMillis() + cfg.pingIntervalMs();

            while (!stop.get() && !stream.closed()) {
                if (System.currentTimeMillis() >= nextPingAt) {
                    stream.ping();
                    nextPingAt = System.currentTimeMillis() + cfg.pingIntervalMs();
                }

                JsonNode frame = stream.poll();
                if (frame == null) {
                    if (stream.closed()) {
                        break;
                    }
                    continue;
                }

                String type = frame.path("type").asText("unknown");
                Metrics.FRAMES.labels(type).inc();

                switch (type) {
                    case "message" -> {
                        recvSeq++;
                        var parsed = TradeFrameParser.toTrade(frame);
                        if (parsed.isPresent()) {
                            emit(producer, sequencer, parsed.get(), stream.connId(), recvSeq);
                            progressed = true;
                        }
                    }
                    case "subscriptions" -> {
                        JsonNode rejected = frame.get("rejected");
                        if (rejected != null && rejected.isArray() && !rejected.isEmpty()) {
                            log.error("구독 거부됨: {}", rejected);
                        } else {
                            log.info("구독 확인됨 connId={}", stream.connId());
                        }
                    }
                    case "error" -> log.error("서버 오류 프레임: {}", frame);
                    case "pong" -> { }
                    default -> log.debug("알 수 없는 프레임 type={}", type);
                }
            }
        }
        return progressed;
    }

    private void emit(Producer<String, byte[]> producer, Sequencer sequencer,
                      TradeFrameParser.ParsedTrade t, String connId, long recvSeq) {
        Sequencer.Assignment a = sequencer.assign(t.symbol(), t.eventMs());
        long ingestMs = System.currentTimeMillis();

        Metrics.SOURCE_LAG.observe(Math.max(0.0, (ingestMs - t.eventMs()) / 1000.0));
        if (a.outOfOrder()) {
            Metrics.OUT_OF_ORDER.labels(t.symbol()).inc();
        }

        TradeRecord rec = new TradeRecord(
                t.symbol(), t.eventMs(), a.seqInMs(), ingestMs,
                t.price(), t.volume(), t.currency(), connId, recvSeq);

        try {
            byte[] value = MAPPER.writeValueAsBytes(rec);
            // 키를 종목으로 둔다. 같은 종목은 같은 파티션 -> 종목 단위 순서 보장.
            producer.send(new ProducerRecord<>(cfg.topic(), t.symbol(), value), (md, ex) -> {
                if (ex != null) {
                    Metrics.PRODUCE_ERRORS.inc();
                    log.error("Kafka 전송 실패", ex);
                } else {
                    Metrics.TRADES.labels(t.symbol()).inc();
                }
            });
        } catch (Exception e) {
            Metrics.PRODUCE_ERRORS.inc();
            log.error("직렬화 실패", e);
        }
    }

    static Producer<String, byte[]> newProducer(Config cfg) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // 순서와 무유실을 우선한다. 수집기가 병목이 되면 그건 그것대로 관측 대상이다.
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return new KafkaProducer<>(p);
    }
}
