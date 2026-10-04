# syntax=docker/dockerfile:1
# Multi-stage build: static binaries on a distroless, non-root runtime image (no shell, no package manager).
# The default (last) stage is the gateway. The fake-cpgw stage is a separate test image for the fake IBKR
# end-to-end path and is never part of the gateway image.

FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY cmd ./cmd
COPY internal ./internal
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/realtime-gateway ./cmd/realtime-gateway \
 && go build -trimpath -ldflags="-s -w" -o /out/stream-probe ./cmd/stream-probe \
 && go build -trimpath -ldflags="-s -w" -o /out/stream-load ./cmd/stream-load \
 && go build -trimpath -ldflags="-s -w" -o /out/api-load ./cmd/api-load \
 && go build -trimpath -ldflags="-s -w" -o /out/fake-cpgw ./cmd/fake-cpgw \
 && mkdir -p /out/fake-cpgw-run

# Test-only image: a FAKE IBKR Client Portal Gateway serving scripted data (make up-ibkr-fake).
FROM gcr.io/distroless/static-debian13:nonroot AS fake-cpgw
COPY --from=build /out/fake-cpgw /usr/local/bin/fake-cpgw
COPY --from=build --chown=65532:65532 /out/fake-cpgw-run /run/fake-cpgw
USER nonroot:nonroot
EXPOSE 5000
ENTRYPOINT ["/usr/local/bin/fake-cpgw"]

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/realtime-gateway /usr/local/bin/realtime-gateway
COPY --from=build /out/stream-probe /usr/local/bin/stream-probe
COPY --from=build /out/stream-load /usr/local/bin/stream-load
COPY --from=build /out/api-load /usr/local/bin/api-load
USER nonroot:nonroot
# 8090: WebSocket and market-data API; 8091: health and metrics (internal)
EXPOSE 8090 8091
ENTRYPOINT ["/usr/local/bin/realtime-gateway"]
CMD ["serve"]
