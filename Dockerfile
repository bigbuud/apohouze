# ── Build stage: compile better-sqlite3 once, keep the toolchain out of the final image
FROM node:20-alpine AS build
RUN apk add --no-cache python3 make g++
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund

# ── Runtime stage
FROM node:20-alpine
WORKDIR /app

ENV NODE_ENV=production \
    PORT=3000 \
    DB_PATH=/data/apohouze.db \
    TZ=Europe/Brussels

COPY --from=build /app/node_modules ./node_modules
COPY package.json server.js ./
COPY db ./db
COPY lib ./lib
COPY public ./public
COPY data ./data

# Runs as the unprivileged "node" user by default. On a Synology you can
# override this with `user: "UID:GID"` in docker-compose.yml so the bind mount
# keeps your own file ownership.
RUN mkdir -p /data && chown -R node:node /data
USER node
VOLUME /data
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "server.js"]
