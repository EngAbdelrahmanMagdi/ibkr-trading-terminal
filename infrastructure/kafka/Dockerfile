FROM apache/kafka:4.3.1 AS kafka
FROM maven:3-eclipse-temurin-24 AS compile
COPY security-patches.pom.xml /src/pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -q -f /src/pom.xml dependency:copy-dependencies -DoutputDirectory=/patches -DincludeArtifactIds=jackson-core,jackson-databind,jackson-dataformat-csv,jackson-dataformat-yaml,jackson-datatype-jdk8,jackson-jakarta-rs-base,jackson-jakarta-rs-json-provider,jackson-module-blackbird,jackson-module-jakarta-xmlbind-annotations,jetty-alpn-client,jetty-client,jetty-http,jetty-io,jetty-security,jetty-server,jetty-session,jetty-util,jetty-ee10-servlet,jetty-ee10-servlets,jline
COPY --from=kafka /opt/kafka/libs /opt/kafka/libs
COPY PrincipalProbe.java /src/PrincipalProbe.java
RUN mkdir /classes && javac --release 17 -cp '/opt/kafka/libs/*' -d /classes /src/PrincipalProbe.java
FROM kafka
USER root
RUN apk add --no-cache libcrypto3=3.5.9-r0 libssl3=3.5.9-r0 openssl=3.5.9-r0 libexpat=2.8.5-r0 p11-kit=0.26.2-r0 p11-kit-trust=0.26.2-r0 sqlite-libs=3.53.4-r0 \
 && rm /opt/kafka/libs/jackson-*-2.21.2.jar /opt/kafka/libs/jetty-*-12.0.34.jar /opt/kafka/libs/jline-3.30.4.jar
COPY --from=compile /patches/ /opt/kafka/libs/
COPY --from=compile /classes /opt/principal-probe
USER appuser
