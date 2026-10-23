FROM apache/kafka:4.3.2 AS kafka
FROM maven:3.9.16-eclipse-temurin-25 AS compile
COPY --from=kafka /opt/kafka/libs /opt/kafka/libs
COPY PrincipalProbe.java /src/PrincipalProbe.java
RUN mkdir /classes && javac --release 17 -cp '/opt/kafka/libs/*' -d /classes /src/PrincipalProbe.java
FROM kafka
USER root
RUN apk add --no-cache libexpat=2.9.0-r0
COPY --from=compile /classes /opt/principal-probe
USER appuser
