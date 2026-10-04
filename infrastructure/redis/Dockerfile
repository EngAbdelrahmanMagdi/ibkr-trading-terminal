FROM redis:8.10.2
RUN apt-get update -qq \
 && apt-get install --only-upgrade -y --no-install-recommends libssl3t64=3.5.7-1~deb13u3 openssl-provider-legacy=3.5.7-1~deb13u3 libpcre2-8-0=10.46-1~deb13u3 \
 && rm -rf /var/lib/apt/lists/*
