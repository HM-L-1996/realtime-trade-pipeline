"""Superset 초기 구성. ClickHouse 연결과 검증용 데이터셋을 만든다.

UI 에서 손으로 만들면 재현이 안 된다. 클러스터를 다시 세웠을 때 같은 것이
나와야 하므로 코드로 둔다 - kind 클러스터 설정을 파일로 둔 것과 같은 이유다.

여러 번 실행해도 안전하다(이미 있으면 갱신한다).
"""

import os

# **모델은 앱 컨텍스트 안에서 임포트한다.**
# 모듈 최상단에서 import 하면 superset.models.helpers 가 app.config 를 읽다가
# "Working outside of application context" 로 죽는다. 실제로 그렇게 실패했다.
from superset.app import create_app

# **환경변수 이름에 RTP_ 접두어를 붙인다.**
#
# 쿠버네티스는 같은 네임스페이스의 서비스마다 <이름>_PORT, <이름>_SERVICE_HOST 같은
# 환경변수를 파드에 자동으로 주입한다. `clickhouse` 라는 서비스가 있으므로
# CLICKHOUSE_PORT 는 이미 `tcp://10.96.18.37:8123` 으로 채워져 있었고,
# 내가 기본값으로 쓰려던 "8123" 이 그것에 덮였다. 결과가 이랬다.
#
#   clickhousedb://rtp:rtp@clickhouse...local:tcp://10.96.18.37:8123/rtp
#
# Superset 은 멀쩡히 떴고 데이터소스만 조용히 깨졌다.
# 이름을 겹치지 않게 두는 것이 근본 해결이다.
CH_USER = os.environ["RTP_CH_USER"]
CH_PASSWORD = os.environ["RTP_CH_PASSWORD"]
CH_HOST = os.environ["RTP_CH_HOST"]
CH_PORT = os.environ["RTP_CH_PORT"]
CH_DB = os.environ["RTP_CH_DB"]

DATABASE_NAME = "ClickHouse (rtp)"

# 공식 캔들과의 차이를 **오차의 모양**으로 볼 수 있게 만든 뷰다.
#
# 지금까지 나온 어긋남이 세 종류였고, 표로는 매번 눈으로 가려야 했다.
#
#   부호가 대칭이고 총량 보존      -> 경계 배정
#   한쪽으로만 음수, 한 윈도 국한  -> 소스 단절
#   한쪽으로만 음수, 앞뒤는 정상   -> 잡이 죽은 채 오프셋만 넘어간 것
#
# 시계열로 겹쳐 보면 즉시 갈린다. 그것이 이 데이터셋의 목적이다.
#
# **CTE(WITH) 로 쓰지 않는다.** Superset 의 가상 데이터셋 검증이
# "Only `SELECT` statements are allowed" 로 거부한다. 같은 내용을
# 서브쿼리로 쓰면 통과한다 - 의미는 같고 첫 토큰만 다르다.
ACCURACY_SQL = """
SELECT
  m.window_start + INTERVAL 9 HOUR        AS window_kst,
  m.symbol                                AS symbol,
  toFloat64(m.my_volume)                  AS my_volume,
  toFloat64(f.off_volume)                 AS off_volume,
  toFloat64(m.my_volume - f.off_volume)   AS volume_diff,
  if(f.off_volume = 0, 0,
     toFloat64(m.my_volume - f.off_volume) / toFloat64(f.off_volume) * 100) AS volume_diff_pct,
  toFloat64(m.my_close - f.off_close)     AS close_diff,
  m.write_count                           AS write_count
FROM (
  SELECT symbol, window_start,
         argMax(volume, ingested_at) AS my_volume,
         argMax(close,  ingested_at) AS my_close,
         count() AS write_count
  FROM rtp.candles_1m
  GROUP BY symbol, window_start
) AS m
INNER JOIN (
  SELECT symbol, window_start,
         argMax(volume, fetched_at) AS off_volume,
         argMax(close,  fetched_at) AS off_close
  FROM rtp.candles_1m_official
  GROUP BY symbol, window_start
) AS f
  ON m.symbol = f.symbol AND m.window_start = f.window_start
"""

DATASETS = [
    ("candle_accuracy", ACCURACY_SQL, "window_kst"),
]


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        uri = f"clickhousedb://{CH_USER}:{CH_PASSWORD}@{CH_HOST}:{CH_PORT}/{CH_DB}"

        database = db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
        if database is None:
            database = Database(database_name=DATABASE_NAME)
            db.session.add(database)
            print(f"데이터베이스 연결 생성: {DATABASE_NAME}")
        else:
            print(f"데이터베이스 연결이 이미 있다: {DATABASE_NAME}")
        database.sqlalchemy_uri = uri
        # SQL Lab 에서 바로 조회할 수 있게 둔다. 검증 쿼리를 손으로 돌려보는 용도다.
        database.expose_in_sqllab = True
        database.allow_ctas = False
        database.allow_cvas = False
        database.allow_dml = False
        db.session.commit()

        for name, sql, main_dttm in DATASETS:
            table = (
                db.session.query(SqlaTable)
                .filter_by(table_name=name, database_id=database.id)
                .one_or_none()
            )
            if table is None:
                table = SqlaTable(table_name=name, database=database, schema=CH_DB)
                db.session.add(table)
                print(f"데이터셋 생성: {name}")
            else:
                print(f"데이터셋 갱신: {name}")
            table.sql = sql.strip()
            table.main_dttm_col = main_dttm
            db.session.commit()
            # 컬럼 메타데이터를 실제 쿼리 결과에서 읽어 온다.
            # 이걸 안 하면 차트를 만들 때 컬럼 목록이 비어 있다.
            try:
                table.fetch_metadata()
                db.session.commit()
                print(f"  컬럼 {len(table.columns)}개 인식")
            except Exception as exc:  # noqa: BLE001
                # 부트스트랩이 실패해도 Superset 자체는 떠야 한다.
                # 다만 조용히 넘기지는 않는다 - 왜 컬럼이 비었는지 알 수 있어야 한다.
                print(f"  !! 컬럼 메타데이터 읽기 실패: {exc}")

        print("부트스트랩 완료")


if __name__ == "__main__":
    main()
