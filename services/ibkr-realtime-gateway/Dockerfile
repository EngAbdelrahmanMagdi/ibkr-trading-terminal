# syntax=docker/dockerfile:1
# Multi-stage build: static binaries on a distroless, non-root runtime image (no shell, no package manager).

FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY cmd ./cmd
COPY internal ./internal
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/realtime-gateway ./cmd/realtime-gateway \
 && go build -trimpath -ldflags="-s -w" -o /out/stream-probe ./cmd/stream-probe \
 && go build -trimpath -ldflags="-s -w" -o /out/stream-load ./cmd/stream-load

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/realtime-gateway /usr/local/bin/realtime-gateway
COPY --from=build /out/stream-probe /usr/local/bin/stream-probe
COPY --from=build /out/stream-load /usr/local/bin/stream-load
USER nonroot:nonroot
# 8090: WebSocket and market-data API; 8091: health (internal)
EXPOSE 8090 8091
ENTRYPOINT ["/usr/local/bin/realtime-gateway"]
CMD ["serve"]
