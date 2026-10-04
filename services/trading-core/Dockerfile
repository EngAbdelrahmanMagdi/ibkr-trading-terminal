# syntax=docker/dockerfile:1
# Multi-stage build: Maven build on the JDK image, then a JRE-only runtime running as a non-root user.

FROM maven:3-eclipse-temurin-24 AS build
WORKDIR /src
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src/main ./src/main
COPY --from=contracts schemas/ /contracts/schemas/
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -DskipTests package

FROM eclipse-temurin:25.0.4.1_1-jre-noble
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=build /src/target/trading-core.jar /app/trading-core.jar
USER 10001:10001
# UTC regardless of the host; temporary files only under /tmp (a tmpfs when the root filesystem is read-only).
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError -Duser.timezone=UTC -Djava.io.tmpdir=/tmp"
# 8080: REST API; 8081: management (health, metrics), internal
EXPOSE 8080 8081
ENTRYPOINT ["java", "-jar", "/app/trading-core.jar"]
