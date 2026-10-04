FROM golang:1.27.1 AS gosu
# Rebuild the same upstream privilege-switching release with the patched Go runtime.
RUN CGO_ENABLED=0 go install -trimpath -ldflags='-s -w -X main.Version=1.19' github.com/tianon/gosu@1.19
FROM postgres:18.6
RUN apt-get update -qq \
 && apt-get install --only-upgrade -y --no-install-recommends libssl3t64=3.5.7-1~deb13u3 openssl=3.5.7-1~deb13u3 openssl-provider-legacy=3.5.7-1~deb13u3 libpcre2-8-0=10.46-1~deb13u3 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=gosu /go/bin/gosu /usr/local/bin/gosu
RUN gosu nobody true
