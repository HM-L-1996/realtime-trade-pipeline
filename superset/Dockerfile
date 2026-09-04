# Superset + ClickHouse 드라이버.
#
# 공식 이미지에는 ClickHouse 드라이버가 없다. 파드 시작 때 pip install 하는 방법도
# 있지만 그러면 **재시작할 때마다 네트워크에 의존**하고, 실패하면 파드가 뜬 채로
# 데이터소스만 안 붙는다 - 이 저장소가 여러 번 당한 "초록불인데 고장난" 모양이다.
# 이미지에 박아 두면 그 경로가 사라진다.
#
#   docker build -f superset/Dockerfile -t rtp-superset:0.1.0 superset/
FROM apache/superset:4.1.1

USER root

# clickhouse-connect 가 Superset 4.x 의 clickhousedb:// 엔진이 쓰는 드라이버다.
RUN pip install --no-cache-dir clickhouse-connect==0.8.15

# 설정 파일. SECRET_KEY 는 여기 두지 않는다 - 환경변수로 Secret 에서 온다.
COPY superset_config.py /app/pythonpath/superset_config.py

USER superset
