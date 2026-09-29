FROM node:24.21.0 AS dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund

FROM node:24.21.0 AS builder
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

FROM node:24.21.0 AS runtime
ENV NODE_ENV=production PORT=3000 HOSTNAME=0.0.0.0
WORKDIR /app
COPY --from=builder --chown=node:node /app/.next/standalone ./
COPY --from=builder --chown=node:node /app/.next/static ./.next/static
USER node
EXPOSE 3000
CMD ["node", "server.js"]
