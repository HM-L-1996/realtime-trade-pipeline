package dev.rtp.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 가격·수량을 스케일된 {@code long} 으로 다룬다.
 *
 * <p>왜 {@link BigDecimal} 을 그대로 쓰지 않는가 - Flink 는 BigDecimal 을 POJO 필드로
 * 인식하지 못하고 Kryo 제네릭 타입으로 떨어뜨린다. <b>Kryo 로 직렬화된 상태는 스키마 진화가
 * 지원되지 않는다.</b> 이 프로젝트는 savepoint 로 상태를 넘기며 잡을 재배포하는 실험을
 * 할 예정이라, 거기서 그대로 막힌다.
 *
 * <p>{@code double} 도 답이 아니다. 소스가 가격을 문자열로 주는 이유가 있고,
 * 부동소수 오차는 그대로 "검증 오차"로 둔갑한다. 공식 캔들과 0.01 차이가 났을 때
 * 그게 내 집계 문제인지 부동소수 문제인지 구분할 수 없게 된다.
 *
 * <p>스케일은 ClickHouse 스키마와 맞춘다 - 가격 {@code Decimal64(4)},
 * 수량 {@code Decimal64(8)}.
 */
public final class Decimals {

    public static final int PRICE_SCALE = 4;
    public static final int VOLUME_SCALE = 8;

    private Decimals() {}

    /**
     * 문자열을 스케일된 long 으로. 스케일을 넘는 자리는 버림한다(반올림하지 않는다) -
     * 거래소가 준 값을 우리가 올려 잡으면 거래대금이 부풀어 검증이 어긋난다.
     *
     * @throws NumberFormatException 숫자가 아니면
     */
    public static long parse(String s, int scale) {
        return new BigDecimal(s.trim())
                .setScale(scale, RoundingMode.DOWN)
                .unscaledValue()
                .longValueExact();
    }

    public static long parsePrice(String s) {
        return parse(s, PRICE_SCALE);
    }

    public static long parseVolume(String s) {
        return parse(s, VOLUME_SCALE);
    }

    /** 스케일된 long 을 다시 십진 문자열로. ClickHouse 에 넣을 때 쓴다. */
    public static String toPlainString(long scaled, int scale) {
        return BigDecimal.valueOf(scaled, scale).toPlainString();
    }

    public static String priceToString(long scaled) {
        return toPlainString(scaled, PRICE_SCALE);
    }

    public static String volumeToString(long scaled) {
        return toPlainString(scaled, VOLUME_SCALE);
    }
}
