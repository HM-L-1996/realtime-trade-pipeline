# 빌드 컨텍스트는 저장소 루트다 (멀티모듈이라 부모 pom 이 필요하다):
#   docker build -f ingester/Dockerfile -t rtp-ingester .
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src

# pom 만 먼저 복사해 의존성 레이어를 캐싱한다.
COPY pom.xml .
COPY common/pom.xml common/
COPY ingester/pom.xml ingester/
COPY aggregator/pom.xml aggregator/
RUN mvn -B -pl common,ingester -am dependency:go-offline -DskipTests

COPY common/src common/src
COPY ingester/src ingester/src
RUN mvn -B -pl ingester -am package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/ingester/target/ingester-0.1.0.jar /app/ingester.jar
EXPOSE 9200
ENTRYPOINT ["java", "-jar", "/app/ingester.jar"]
