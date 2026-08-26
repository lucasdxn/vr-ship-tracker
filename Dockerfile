# AIS relay + static UI server.
# relay.js serves the HTML pages and the WebSocket relay on a single port (PORT).
FROM node:20-alpine

WORKDIR /app

# Install dependencies first so this layer is cached unless the manifest changes.
COPY package.json package-lock.json ./
RUN npm install --omit=dev

# Copy application sources (HTML pages, relay).
COPY relay.js ./
COPY ais_data_stream.html ship_tracker.html ./

# relay.js reads PORT from the environment; host on 8080.
ENV PORT=8080
EXPOSE 8080

CMD ["node", "relay.js"]
