# syntax=docker/dockerfile:1
FROM python:3.14.7-slim AS build
WORKDIR /build
COPY requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip pip install --require-hashes --prefix=/install -r requirements.txt

FROM python:3.14.7-slim
WORKDIR /app
COPY --from=build /install /usr/local
COPY --from=contracts schemas/ /app/contracts/schemas/
COPY src/ /app/src/
ENV PYTHONPATH=/app/src PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
EXPOSE 8092
ENTRYPOINT ["python", "-m", "ai_insights"]
