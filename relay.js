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

// Only error and connection messages are persisted to disk; everything else
// is just printed to the console. The file is cleared once it reaches 4 GB so
// it can never grow unbounded.
const LOG_MAX_BYTES = 4 * 1024 * 1024 * 1024;
let logStream = fs.createWriteStream(LOG_PATH, { flags: 'a' });
let logBytes = (() => { try { return fs.statSync(LOG_PATH).size; } catch (_) { return 0; } })();

function fileLog(line) {
  const entry = '[' + new Date().toISOString() + '] ' + line + '\n';
  const size = Buffer.byteLength(entry);
  if (logBytes + size > LOG_MAX_BYTES) {
    // Clear the file by reopening in truncate mode.
    logStream.end();
    logStream = fs.createWriteStream(LOG_PATH, { flags: 'w' });
    logBytes = 0;
  }
  logStream.write(entry);
  logBytes += size;
}
fileLog('---- relay session started ----');

// Serve the two HTML files (plus any sibling .html/.js/.css) so that the
// data stream and globe tracker live on the same http://localhost origin —
// required for the BroadcastChannel that links them.
const STATIC_FILES = {
  '/':                         { file: 'ais_data_stream.html',    type: 'text/html; charset=utf-8' },
  '/ais_data_stream.html':     { file: 'ais_data_stream.html',    type: 'text/html; charset=utf-8' },
  '/globe_ship_tracker.html':  { file: 'globe_ship_tracker.html', type: 'text/html; charset=utf-8' },
};

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  const entry = STATIC_FILES[url];
  if (entry) {
    const full = path.join(__dirname, entry.file);
    fs.readFile(full, (err, data) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Failed to read ' + entry.file + ': ' + err.message);
        return;
      }
      res.writeHead(200, { 'Content-Type': entry.type });
      res.end(data);
    });
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end(
    'Not found: ' + url + '\n\n' +
    'AIS relay is running.\n' +
    'UI:        http://localhost:' + PORT + '/\n' +
    'Globe:     http://localhost:' + PORT + '/globe_ship_tracker.html\n' +
    'WebSocket: ws://localhost:' + PORT + '/v0/stream\n' +
    'Upstream:  ' + UPSTREAM + '\n'
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
    console.log('[' + ts() + '] UP #' + upMsgCount + ' ' + text);
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
    console.log('[' + ts() + '] CLIENT->UP ' + text);
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(text);
      bytesUp += text.length;
    } else if (upstream.readyState === WebSocket.CONNECTING) {
      pending.push(text);
    } else {
      console.warn('[' + ts() + '] dropping client message; upstream not open');
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
  console.log('AIS relay listening on http://localhost:' + PORT + '/');
  console.log('  UI:        http://localhost:' + PORT + '/');
  console.log('  Globe:     http://localhost:' + PORT + '/globe_ship_tracker.html');
  console.log('  WebSocket: ws://localhost:' + PORT + '/v0/stream');
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
