FROM node:26.10.0 AS dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund

FROM node:26.10.0 AS builder
ENV NEXT_TELEMETRY_DISABLED=1
WORKDIR /app
COPY --from=dependencies /app/node_modules ./node_modules
COPY . .
ARG NEXT_PUBLIC_CORE_ORIGIN=http://localhost:18080
ARG NEXT_PUBLIC_GATEWAY_ORIGIN=http://localhost:18090
ARG NEXT_PUBLIC_GATEWAY_WS_URL=ws://localhost:18090/ws
ENV NEXT_PUBLIC_CORE_ORIGIN=$NEXT_PUBLIC_CORE_ORIGIN
ENV NEXT_PUBLIC_GATEWAY_ORIGIN=$NEXT_PUBLIC_GATEWAY_ORIGIN
ENV NEXT_PUBLIC_GATEWAY_WS_URL=$NEXT_PUBLIC_GATEWAY_WS_URL
RUN npm run build

FROM node:26.10.0-trixie-slim AS runtime
# Apply compatible distribution security updates without changing the Node runtime.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libssl3t64=3.5.7-1~deb13u3 openssl=3.5.7-1~deb13u3 openssl-provider-legacy=3.5.7-1~deb13u3 libpcre2-8-0=10.46-1~deb13u3 \
 && rm -rf /var/lib/apt/lists/*
# Only Node is needed to run the standalone server; omit build tools and package managers.
RUN rm -rf /usr/local/lib/node_modules/npm /opt/yarn-* \
 && rm -f /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/yarn /usr/local/bin/yarnpkg
ENV NODE_ENV=production PORT=3000 HOSTNAME=0.0.0.0 NEXT_TELEMETRY_DISABLED=1
WORKDIR /app
COPY --from=builder --chown=node:node /app/.next/standalone ./
COPY --from=builder --chown=node:node /app/.next/static ./.next/static
USER node
EXPOSE 3000
CMD ["node", "server.js"]
