# syntax=docker/dockerfile:1
FROM python:3.14.7-slim AS build
WORKDIR /build
COPY requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip pip install --require-hashes --prefix=/install -r requirements.txt

FROM python:3.14.7-slim
# Runtime needs neither package installation nor pip's vendored development libraries.
RUN apt-get update -qq \
 && apt-get install --only-upgrade -y --no-install-recommends \
    libssl3t64=3.5.7-1~deb13u3 openssl=3.5.7-1~deb13u3 \
    openssl-provider-legacy=3.5.7-1~deb13u3 libpcre2-8-0=10.46-1~deb13u3 \
 && rm -rf /var/lib/apt/lists/* \
 && python -m pip uninstall -y pip
WORKDIR /app
COPY --from=build /install /usr/local
COPY --from=contracts schemas/ /app/contracts/schemas/
COPY src/ /app/src/
ENV PYTHONPATH=/app/src PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
EXPOSE 8092
ENTRYPOINT ["python", "-m", "ai_insights"]
