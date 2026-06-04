/*
 * AIS relay: bridges browser WebSocket clients to wss://stream.aisstream.io.
 *
 * aisstream.io's WebSocket endpoint rejects direct browser connections,
 * so their own browser example (github.com/aisstream/example) routes
 * through a local relay. This is that relay.
 *
 * Usage:
 *   npm install
 *   node relay.js
 *
 * Then open ais_data_stream.html and Connect; the HTML now points at
 * ws://localhost:3333/v0/stream instead of wss://stream.aisstream.io directly.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT) || 3333;
const UPSTREAM = 'wss://stream.aisstream.io/v0/stream';
const LOG_PATH = path.join(__dirname, 'stream_log.txt');
const INSECURE_TLS = process.env.INSECURE_TLS === '1';

const logStream = fs.createWriteStream(LOG_PATH, { flags: 'a' });
function fileLog(line) {
  logStream.write('[' + new Date().toISOString() + '] ' + line + '\n');
}
fileLog('---- relay session started ----');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(
    'AIS relay is running.\n' +
    'Connect a WebSocket to ws://localhost:' + PORT + '/v0/stream\n' +
    'Upstream: ' + UPSTREAM + '\n'
  );
});

const wss = new WebSocket.Server({ server });

function ts() { return new Date().toISOString(); }

wss.on('connection', (client, req) => {
  const peer = req.socket.remoteAddress;
  console.log('[' + ts() + '] client connected from ' + peer);
  fileLog('client connected from ' + peer);

  const upstream = new WebSocket(UPSTREAM, INSECURE_TLS ? { rejectUnauthorized: false } : undefined);
  const pending = [];
  let bytesUp = 0, bytesDown = 0;
  let upMsgCount = 0;

  upstream.on('open', () => {
    console.log('[' + ts() + '] upstream open; flushing ' + pending.length + ' queued message(s)');
    fileLog('UPSTREAM OPEN (handshake to aisstream.io succeeded)');
    while (pending.length && upstream.readyState === WebSocket.OPEN) {
      const msg = pending.shift();
      upstream.send(msg);
      bytesUp += msg.length || 0;
    }
  });

  upstream.on('message', (data) => {
    upMsgCount++;
    // Force a text frame: aisstream sends JSON as text, `ws` decodes it
    // into a Buffer, and forwarding the Buffer would emit a binary frame
    // which the browser surfaces as a Blob (not a string). JSON.parse on
    // a Blob fails silently and the browser sees "no messages".
    const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data);
    if (client.readyState === WebSocket.OPEN) {
      client.send(text);
      bytesDown += text.length;
    }
    fileLog('UP #' + upMsgCount + ' ' + text);
  });

  upstream.on('close', (code, reason) => {
    const r = reason.toString();
    console.log('[' + ts() + '] upstream closed code=' + code + ' reason="' + r + '"');
    fileLog('UPSTREAM CLOSED code=' + code + ' reason="' + r + '" (received ' + upMsgCount + ' message(s))');
    try { client.close(code <= 4999 ? code : 1011, r.slice(0, 120)); } catch (_) {}
  });

  upstream.on('error', (err) => {
    console.error('[' + ts() + '] upstream error: ' + err.message);
    fileLog('UPSTREAM ERROR: ' + err.message);
  });

  client.on('message', (data) => {
    // Same treatment in this direction: send as text frame upstream.
    const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data);
    fileLog('CLIENT->UP ' + text);
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(text);
      bytesUp += text.length;
    } else if (upstream.readyState === WebSocket.CONNECTING) {
      pending.push(text);
    } else {
      console.warn('[' + ts() + '] dropping client message; upstream not open');
      fileLog('DROPPED client message; upstream not open');
    }
  });

  client.on('close', () => {
    console.log('[' + ts() + '] client closed (up=' + bytesUp + 'B, down=' + bytesDown + 'B)');
    fileLog('client closed (up=' + bytesUp + 'B, down=' + bytesDown + 'B, upMsgs=' + upMsgCount + ')');
    try { upstream.close(); } catch (_) {}
  });

  client.on('error', (err) => {
    console.error('[' + ts() + '] client error: ' + err.message);
    fileLog('CLIENT ERROR: ' + err.message);
  });
});

server.listen(PORT, () => {
  console.log('AIS relay listening on ws://localhost:' + PORT + '/v0/stream');
  console.log('Forwarding to ' + UPSTREAM);
  console.log('Node version: ' + process.version);
  console.log('Appending stream log to ' + LOG_PATH);
  if (INSECURE_TLS) {
    console.log('\n  ⚠  INSECURE_TLS=1 — upstream certificate verification is DISABLED.');
    console.log('     Use this only as a temporary workaround. Upgrade Node.js to fix properly.\n');
    fileLog('INSECURE_TLS enabled');
  }
  console.log('Press Ctrl+C to stop.');
});

process.on('SIGINT', () => {
  fileLog('---- relay session ended (SIGINT) ----');
  logStream.end(() => process.exit(0));
});
