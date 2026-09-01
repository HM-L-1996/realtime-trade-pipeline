# Flink Application 모드용 잡 이미지.
# Operator 는 잡 jar 이 이미지 안에 있는 것을 전제로 한다(jarURI: local://...).
#
# 빌드 컨텍스트는 저장소 루트다:
#   ./mvnd.sh -pl aggregator -am package -DskipTests
#   docker build -f aggregator/Dockerfile -t rtp-aggregator:0.1.0 .
FROM flink:1.20-scala_2.12-java17

# S3 파일시스템 플러그인. 체크포인트/세이브포인트를 MinIO 에 둔다.
# JobManager 와 TaskManager 가 다른 노드에 뜨는데 kind 기본 StorageClass 는
# ReadWriteOnce 라 공유가 안 된다. 오브젝트 스토리지가 유일한 선택지다.
#
# Flink 는 plugins/ 아래 **각자 디렉터리**에 있는 것만 로드한다.
# opt/ 에 있는 것을 그냥 lib/ 로 복사하면 클래스 충돌이 난다.
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
 && cp /opt/flink/opt/flink-s3-fs-presto-*.jar /opt/flink/plugins/s3-fs-presto/

# 잡 jar. usrlib 에 두면 Operator 가 user classloader 로 올린다.
RUN mkdir -p /opt/flink/usrlib
COPY aggregator/target/aggregator-0.1.0.jar /opt/flink/usrlib/aggregator.jar
