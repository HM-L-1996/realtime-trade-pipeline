#!/usr/bin/env bash
# 로컬에 JDK/Maven 을 깔지 않고 Docker 안에서 Maven 을 돌린다.
# ~/.m2 를 named volume 으로 캐싱해 두 번째 빌드부터는 의존성을 다시 받지 않는다.
set -euo pipefail
docker run --rm \
  -v "$(pwd)":/work \
  -v rtp-m2:/root/.m2 \
  -w /work \
  maven:3.9-eclipse-temurin-17 \
  mvn "$@"
