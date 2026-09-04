"""Superset 설정.

이 프로젝트에서 Superset 이 답하는 질문은 Grafana 와 다르다.

  Grafana   Prometheus 를 본다  -> "파이프라인이 지금 건강한가"
  Superset  ClickHouse 를 본다  -> "결과가 맞았는가"

그래서 둘이 함께 있는 것이 중복이 아니다.
"""

import os

# 자격증명은 코드에 두지 않는다. 없으면 뜨지 않게 해서 조용히 기본값으로
# 도는 상황을 막는다 - 약한 키로 떠 있는 것이 안 뜨는 것보다 나쁘다.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# 메타데이터(대시보드·차트 정의)를 담는 곳. PVC 위의 SQLite 다.
#
# emptyDir 로 두면 파드가 다시 뜰 때마다 만들어 둔 대시보드가 사라진다.
# Prometheus 를 emptyDir 로 뒀다가 장애 실험 세 건의 파형을 잃은 적이 있어
# 처음부터 볼륨을 붙인다. 단일 사용자 로컬 환경이라 SQLite 로 충분하다.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# 예제 대시보드를 만들지 않는다. 이 프로젝트의 데이터만 보이는 편이 낫다.
SUPERSET_LOAD_EXAMPLES = False

# 로컬 전용. 외부에 노출하지 않는다.
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

FEATURE_FLAGS = {
    # 가상 데이터셋(SQL 로 정의한 것)을 쓴다. 정확도 대조 쿼리가 조인이라 필요하다.
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# ClickHouse 쪽 조회가 무거울 수 있다. 넉넉히 둔다.
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120
