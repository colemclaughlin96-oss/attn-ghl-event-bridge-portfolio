FROM node:24-bookworm-slim
WORKDIR /app
COPY --chown=node:node package.json ./
COPY --chown=node:node src ./src
COPY --chown=node:node docs/INBOUND-RELIABILITY-PROMPT.txt docs/SERVER-BOT-PROMPT.txt ./docs/
RUN mkdir -p /data && chown node:node /data
USER node
ENV NODE_ENV=production PORT=3000 DB_PATH=/data/bridge.sqlite
EXPOSE 3000
CMD ["node", "src/server.mjs"]


