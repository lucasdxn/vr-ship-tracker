/*
 * AIS relay: bridges browser WebSocket clients to three different upstream
 * AIS APIs, picked per-connection by the client (see ais_data_stream.html's
 * provider dropdown):
 *
 *  - aisstream.io: a raw WebSocket relay. Their endpoint rejects direct
 *    browser connections, so their own browser example
 *    (github.com/aisstream/example) routes through a local relay - this is
 *    that relay.
 *  - BarentsWatch (barentswatch.no): OAuth2 client_credentials over HTTPS,
 *    then a chunked HTTP streaming response (newline-delimited JSON), not a
 *    WebSocket at all. The relay does the OAuth exchange (the client secret
 *    must never reach the browser) and translates each line into the same
 *    message shape aisstream.io uses.
 *  - Digitraffic (digitraffic.fi, Fintraffic's open data service): no
 *    account, no key, nothing to configure at all - it's genuinely open
 *    REST data (CC BY 4.0), covering Finnish/Baltic waters. Polled and
 *    translated the same way as the other two, just without any credentials
 *    to pass through. It also has no bounding-box filter of its own, so the
 *    relay filters the (small, ~800 vessel) result set by bbox itself.
 *
 * All three get translated into the exact same message shape aisstream.io
 * sends, so ais_data_stream.html and everything downstream of it
 * (BroadcastChannel, the globe tracker, the VR tracker) never has to know
 * which provider is actually feeding it.
 *
 * A fourth provider, Kpler (via Spire Maritime's "Messages API"), was
 * integrated and then removed: the integration was only ever verified
 * against its failure path (a deliberately invalid bearer token correctly
 * rejected), since no working credentials were available, and it was built
 * against Spire's documented API on the unconfirmed assumption that it's
 * equivalent to what Kpler's own developer portal exposes to a real account.
 * Dropped rather than shipped as an unverified integration.
 *
 * Usage:
 *   npm install
 *   node relay.js
 *
 * Then open ais_data_stream.html and Connect; the HTML points at
 * ws://localhost:3333/v0/stream (aisstream.io), .../v0/stream/barentswatch,
 * or .../v0/stream/digitraffic depending on what's picked in the modal.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { Readable } = require('stream');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT) || 3333;
const UPSTREAM = 'wss://stream.aisstream.io/v0/stream';
const BW_TOKEN_URL = 'https://id.barentswatch.no/connect/token';
const BW_STREAM_URL = 'https://live.ais.barentswatch.no/live/v1/combined?modelType=Full';
const DT_LOCATIONS_URL = 'https://meri.digitraffic.fi/api/ais/v1/locations';
const DT_VESSELS_URL = 'https://meri.digitraffic.fi/api/ais/v1/vessels';
const DT_LOCATIONS_POLL_MS = 5000;
const DT_VESSELS_POLL_MS = 60000; // static data barely changes, no need to refetch all ~800 vessels often
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

/* ============================================================ */
/* AIS message history log (for the VR tracker's playback mode)             */
/* ============================================================ */
// stream_log.txt above only ever recorded connection/error events - the
// actual AIS traffic only went to console.log, so there was nothing to
// replay. This is a second, separate append-only log, one JSON line per
// message actually produced by any of the three providers: {ts, msg}, where
// msg is the exact aisstream.io-shaped object the browser would have
// received. vr_ship_tracker.html's playback mode reads this back through the
// /api/history endpoint below.
const HISTORY_LOG_PATH = path.join(__dirname, 'ais_message_log.jsonl');
const HISTORY_LOG_MAX_BYTES = 2 * 1024 * 1024 * 1024; // 2 GB - same truncate-on-overflow approach as stream_log.txt above

let historyLogStream = fs.createWriteStream(HISTORY_LOG_PATH, { flags: 'a' });
let historyLogBytes = (() => { try { return fs.statSync(HISTORY_LOG_PATH).size; } catch (_) { return 0; } })();

// kept current on every write so /api/history/range is an O(1) lookup
// instead of a full file scan on every request from the client
const historyStats = { count: 0, firstTs: null, lastTs: null };

function logAisMessage(msg) {
  const entryTs = Date.now();
  const entry = JSON.stringify({ ts: entryTs, msg }) + '\n';
  const size = Buffer.byteLength(entry);
  if (historyLogBytes + size > HISTORY_LOG_MAX_BYTES) {
    historyLogStream.end();
    historyLogStream = fs.createWriteStream(HISTORY_LOG_PATH, { flags: 'w' });
    historyLogBytes = 0;
    historyStats.count = 0;
    historyStats.firstTs = null;
  }
  historyLogStream.write(entry);
  historyLogBytes += size;
  historyStats.count++;
  if (historyStats.firstTs == null) historyStats.firstTs = entryTs;
  historyStats.lastTs = entryTs;
}

// one-time startup scan so historyStats (and therefore /api/history/range)
// is accurate for lines written by an *earlier* relay process too, not just
// this one - streamed line-by-line so a large existing log doesn't get
// loaded into memory all at once just to count it
function scanHistoryLogBounds() {
  return new Promise((resolve) => {
    if (historyLogBytes === 0) { resolve(); return; }
    const rl = readline.createInterface({ input: fs.createReadStream(HISTORY_LOG_PATH), crlfDelay: Infinity });
    let count = 0, firstTs = null, lastTs = null;
    rl.on('line', (line) => {
      if (!line) return;
      let entry;
      try { entry = JSON.parse(line); } catch { return; }
      if (typeof entry.ts !== 'number') return;
      count++;
      if (firstTs == null) firstTs = entry.ts;
      lastTs = entry.ts;
    });
    rl.on('close', () => {
      historyStats.count = count;
      historyStats.firstTs = firstTs;
      historyStats.lastTs = lastTs;
      console.log('[' + ts() + '] history log: ' + count + ' message(s) on disk' +
        (firstTs ? ', spanning ' + new Date(firstTs).toISOString() + ' to ' + new Date(lastTs).toISOString() : ''));
      resolve();
    });
    rl.on('error', () => resolve());
  });
}

// Serve the three HTML files (plus any sibling .html/.js/.css) so that the
// data stream, globe tracker and VR tracker all live on the same
// http://localhost origin — required for the BroadcastChannel that links them.
const STATIC_FILES = {
  '/':                         { file: 'ais_data_stream.html',    type: 'text/html; charset=utf-8' },
  '/ais_data_stream.html':     { file: 'ais_data_stream.html',    type: 'text/html; charset=utf-8' },
  '/globe_ship_tracker.html':  { file: 'globe_ship_tracker.html', type: 'text/html; charset=utf-8' },
  '/vr_ship_tracker.html':     { file: 'vr_ship_tracker.html',    type: 'text/html; charset=utf-8' },
  '/ship_tracker.html':        { file: 'ship_tracker.html',       type: 'text/html; charset=utf-8' },
};

const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, 'http://' + (req.headers.host || 'localhost'));
  const url = parsedUrl.pathname;

  // cheap: historyStats is kept current in memory, no file access per request
  if (url === '/api/history/range') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ count: historyStats.count, firstTs: historyStats.firstTs, lastTs: historyStats.lastTs }));
    return;
  }

  // returns every logged message with since <= ts <= until, oldest first,
  // each with its original _ts (epoch ms) attached so the client can pace
  // replay against real elapsed time. capped at `limit` (default/max
  // 50000/200000) so a wide time window can't hand the browser an
  // unbounded response; `truncated: true` tells the client there's more.
  if (url === '/api/history') {
    const since = Number(parsedUrl.searchParams.get('since'));
    const until = Number(parsedUrl.searchParams.get('until'));
    const limit = Math.min(Number(parsedUrl.searchParams.get('limit')) || 50000, 200000);
    if (!Number.isFinite(since) || !Number.isFinite(until)) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'since and until query params (epoch ms) are required' }));
      return;
    }
    const messages = [];
    let truncated = false;
    const rl = readline.createInterface({ input: fs.createReadStream(HISTORY_LOG_PATH), crlfDelay: Infinity });
    rl.on('line', (line) => {
      if (!line || messages.length >= limit) return;
      let entry;
      try { entry = JSON.parse(line); } catch { return; }
      if (typeof entry.ts !== 'number' || entry.ts < since || entry.ts > until) return;
      messages.push(Object.assign({ _ts: entry.ts }, entry.msg));
      if (messages.length >= limit) { truncated = true; rl.close(); }
    });
    rl.on('close', () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ messages, count: messages.length, truncated }));
    });
    rl.on('error', (err) => {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    });
    return;
  }

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
    'Combined:  http://localhost:' + PORT + '/ship_tracker.html\n' +
    'Globe:     http://localhost:' + PORT + '/globe_ship_tracker.html\n' +
    'VR:        http://localhost:' + PORT + '/vr_ship_tracker.html\n' +
    'WebSocket: ws://localhost:' + PORT + '/v0/stream\n' +
    'History:   http://localhost:' + PORT + '/api/history?since=<ms>&until=<ms>\n' +
    'Upstream:  ' + UPSTREAM + '\n'
  );
});

const wss = new WebSocket.Server({ server });

function ts() { return new Date().toISOString(); }

/* ============================================================ */
/* BarentsWatch bridge                                                      */
/* ============================================================ */

// exchanges a client id/secret for a bearer token good for ~1h (see
// developer.barentswatch.no/docs/appreg). never log clientSecret or the
// token itself - only aisstream's key logging is a pre-existing wart in
// this file, no reason to add a second one for the new provider
async function getBarentsWatchToken(clientId, clientSecret) {
  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: clientId,
    client_secret: clientSecret,
    scope: 'ais'
  });
  const res = await fetch(BW_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString()
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error('token request failed: HTTP ' + res.status + ' ' + detail.slice(0, 200));
  }
  const json = await res.json();
  if (!json.access_token) throw new Error('token response had no access_token');
  return json.access_token;
}

// BarentsWatch wants a GeoJSON polygon, not a bbox - bbox here is the same
// [south, west, north, east] the region dropdown already produces
function bboxToPolygon([s, w, n, e]) {
  return { type: 'Polygon', coordinates: [[[w, s], [e, s], [e, n], [w, n], [w, s]]] };
}

// translates one BarentsWatch "combined" record into the exact message
// shapes aisstream.io sends, so ais_data_stream.html's ws.onmessage doesn't
// need a single line of provider-specific parsing. can emit up to two
// messages per record since a combined record carries both position and
// static fields that aisstream.io normally sends separately.
function comboToAisstreamMessages(c) {
  const out = [];
  const hasStatic = c.name || c.shipType != null || c.destination || c.dimensionA != null;
  if (hasStatic) {
    out.push({
      MessageType: 'ShipStaticData',
      MetaData: { MMSI: c.mmsi, ShipName: c.name },
      Message: { ShipStaticData: {
        Name: c.name, Type: c.shipType, Destination: c.destination,
        Dimension: { A: c.dimensionA, B: c.dimensionB, C: c.dimensionC, D: c.dimensionD }
      } }
    });
  }
  if (c.latitude != null && c.longitude != null) {
    out.push({
      MessageType: 'PositionReport',
      MetaData: { MMSI: c.mmsi, ShipName: c.name, latitude: c.latitude, longitude: c.longitude },
      Message: { PositionReport: { Latitude: c.latitude, Longitude: c.longitude, Cog: c.courseOverGround, Sog: c.speedOverGround } }
    });
  }
  return out;
}

// opens the BarentsWatch streaming request and pipes translated messages to
// the browser client until either side hangs up. unlike aisstream.io this
// is plain chunked HTTP, not a socket - Readable.fromWeb turns the fetch
// Response body (a WHATWG stream) into a regular Node stream we can listen
// to the same way as everything else in this file
async function startBarentsWatchStream(token, bbox, client) {
  const controller = new AbortController();
  client.on('close', () => controller.abort());

  const res = await fetch(BW_STREAM_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({
      geometry: bboxToPolygon(bbox),
      includePosition: true, includeStatic: true,
      includeAton: false, includeSafetyRelated: false, includeBinaryBroadcastMetHyd: false,
      downsample: false
    }),
    signal: controller.signal
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error('AIS stream request failed: HTTP ' + res.status + ' ' + detail.slice(0, 200));
  }
  console.log('[' + ts() + '] barentswatch stream open');
  fileLog('BARENTSWATCH STREAM OPEN');

  let buf = '';
  let msgCount = 0;
  const nodeStream = Readable.fromWeb(res.body);
  nodeStream.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    let idx;
    while ((idx = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      let combo;
      try { combo = JSON.parse(line); } catch { continue; }
      msgCount++;
      for (const msg of comboToAisstreamMessages(combo)) {
        if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(msg));
        logAisMessage(msg);
      }
    }
  });
  nodeStream.on('end', () => {
    console.log('[' + ts() + '] barentswatch stream ended (received ' + msgCount + ' record(s))');
    fileLog('BARENTSWATCH STREAM ENDED (received ' + msgCount + ' record(s))');
    try { client.close(1000, 'stream ended'); } catch (_) {}
  });
  nodeStream.on('error', (err) => {
    if (controller.signal.aborted) return; // expected - the browser client disconnected
    console.error('[' + ts() + '] barentswatch stream error: ' + err.message);
    fileLog('BARENTSWATCH STREAM ERROR: ' + err.message);
    try { client.close(1011, 'stream error'); } catch (_) {}
  });
}

/* ============================================================ */
/* Digitraffic bridge                                                       */
/* ============================================================ */

function withinBbox(lat, lon, [s, w, n, e]) {
  return lat >= s && lat <= n && lon >= w && lon <= e;
}

// Digitraffic (no auth, no key - see meri.digitraffic.fi/swagger) has two
// separate endpoints, same split as everywhere else in this file: locations
// (position, changes constantly) and vessels (name/type/dimensions, barely
// changes). Static data doesn't come with a position filter at all, so it's
// just cached by mmsi here and attached the first time that mmsi shows up
// inside the requested bbox - no point pushing static data to the browser
// for the hundreds of vessels that will never actually be in view.
async function startDigitrafficStream(bbox, client) {
  let stopped = false;
  client.on('close', () => { stopped = true; });
  const metadataByMmsi = new Map();
  const staticSent = new Set();

  async function refreshMetadata() {
    const res = await fetch(DT_VESSELS_URL, { headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error('vessels request failed: HTTP ' + res.status);
    const rows = await res.json();
    for (const v of rows) { if (v.mmsi != null) metadataByMmsi.set(v.mmsi, v); }
  }

  function sendStaticIfNew(mmsi) {
    if (staticSent.has(mmsi)) return;
    const v = metadataByMmsi.get(mmsi);
    if (!v) return; // no metadata cached yet for this vessel - try again once refreshMetadata() catches up
    staticSent.add(mmsi);
    const msg = {
      MessageType: 'ShipStaticData',
      MetaData: { MMSI: mmsi, ShipName: v.name },
      Message: { ShipStaticData: {
        Name: v.name, Type: v.shipType, Destination: v.destination,
        Dimension: { A: v.referencePointA, B: v.referencePointB, C: v.referencePointC, D: v.referencePointD }
      } }
    };
    if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(msg));
    logAisMessage(msg);
  }

  async function pollLocations() {
    const res = await fetch(DT_LOCATIONS_URL, { headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error('locations request failed: HTTP ' + res.status);
    const geo = await res.json();
    const features = Array.isArray(geo.features) ? geo.features : [];
    for (const f of features) {
      const coords = f.geometry && f.geometry.coordinates; // GeoJSON: [lon, lat]
      if (!coords) continue;
      const [lon, lat] = coords;
      if (!withinBbox(lat, lon, bbox)) continue;
      sendStaticIfNew(f.mmsi);
      const p = f.properties || {};
      const msg = {
        MessageType: 'PositionReport',
        MetaData: { MMSI: f.mmsi, ShipName: (metadataByMmsi.get(f.mmsi) || {}).name, latitude: lat, longitude: lon },
        Message: { PositionReport: {
          Latitude: lat, Longitude: lon,
          // 360/102.3 are this API's own "not available" sentinels, same idea as the other providers
          Cog: (p.cog != null && p.cog < 360) ? p.cog : undefined,
          Sog: (p.sog != null && p.sog < 102.3) ? p.sog : undefined
        } }
      };
      if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(msg));
      logAisMessage(msg);
    }
  }

  await refreshMetadata().catch((err) => {
    console.error('[' + ts() + '] digitraffic metadata warmup failed (continuing without names): ' + err.message);
  });
  await pollLocations(); // this one's allowed to throw - it's the real "does this even work" check

  console.log('[' + ts() + '] digitraffic polling started');
  fileLog('DIGITRAFFIC STREAM OPEN');

  let sinceMetadataRefresh = 0;
  let failures = 0;
  while (!stopped) {
    await new Promise((resolve) => setTimeout(resolve, DT_LOCATIONS_POLL_MS));
    if (stopped) break;
    sinceMetadataRefresh += DT_LOCATIONS_POLL_MS;
    try {
      await pollLocations();
      failures = 0;
    } catch (err) {
      failures++;
      console.error('[' + ts() + '] digitraffic poll error (' + failures + '/5): ' + err.message);
      fileLog('DIGITRAFFIC POLL ERROR: ' + err.message);
      if (failures >= 5) {
        fileLog('DIGITRAFFIC STREAM GIVING UP after 5 consecutive poll failures');
        try { client.close(1011, 'too many consecutive poll failures'); } catch (_) {}
        return;
      }
    }
    if (sinceMetadataRefresh >= DT_VESSELS_POLL_MS) {
      sinceMetadataRefresh = 0;
      refreshMetadata().catch((err) => fileLog('DIGITRAFFIC METADATA REFRESH ERROR: ' + err.message));
    }
  }
}

// shared plumbing for the two "bridged" providers (BarentsWatch,
// Digitraffic): wait for the browser's one subscribe message, hand it to
// whichever bridge function actually knows how to talk to that provider,
// and turn any failure (bad credentials, bad bbox, upstream down) into a
// client-side close the existing onclose hint logic in ais_data_stream.html
// already knows how to explain
function handleBridgedProvider(client, label, runBridge) {
  client.once('message', async (raw) => {
    let sub;
    try { sub = JSON.parse(raw.toString()); } catch { client.close(1008, 'bad subscribe message'); return; }
    try {
      await runBridge(sub);
    } catch (err) {
      console.error('[' + ts() + '] ' + label + ' error: ' + err.message);
      fileLog(label.toUpperCase() + ' ERROR: ' + err.message);
      try { client.close(4001, String(err.message).slice(0, 120)); } catch (_) {}
    }
  });
  client.on('error', (err) => {
    console.error('[' + ts() + '] client error: ' + err.message);
    fileLog('CLIENT ERROR: ' + err.message);
  });
}

wss.on('connection', (client, req) => {
  const peer = req.socket.remoteAddress;
  console.log('[' + ts() + '] client connected from ' + peer);
  fileLog('client connected from ' + peer);

  const streamPath = (req.url || '').split('?')[0];

  if (streamPath === '/v0/stream/barentswatch') {
    handleBridgedProvider(client, 'barentswatch', async (sub) => {
      const token = await getBarentsWatchToken(sub.clientId, sub.clientSecret);
      await startBarentsWatchStream(token, sub.bbox, client);
    });
    return;
  }

  if (streamPath === '/v0/stream/digitraffic') {
    handleBridgedProvider(client, 'digitraffic', async (sub) => {
      await startDigitrafficStream(sub.bbox, client);
    });
    return;
  }

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
    try { logAisMessage(JSON.parse(text)); } catch (_) { /* not JSON - nothing to log, already forwarded above regardless */ }
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

server.listen(PORT, async () => {
  console.log('AIS relay listening on http://localhost:' + PORT + '/');
  console.log('  UI:        http://localhost:' + PORT + '/');
  console.log('  Combined:  http://localhost:' + PORT + '/ship_tracker.html');
  console.log('  Globe:     http://localhost:' + PORT + '/globe_ship_tracker.html');
  console.log('  VR:        http://localhost:' + PORT + '/vr_ship_tracker.html');
  console.log('  WebSocket: ws://localhost:' + PORT + '/v0/stream');
  console.log('  History:   http://localhost:' + PORT + '/api/history');
  console.log('Forwarding to ' + UPSTREAM);
  console.log('Node version: ' + process.version);
  console.log('Appending stream log to ' + LOG_PATH);
  console.log('Appending AIS message history to ' + HISTORY_LOG_PATH);
  if (INSECURE_TLS) {
    console.log('\n  ⚠  INSECURE_TLS=1 — upstream certificate verification is DISABLED.');
    console.log('     Use this only as a temporary workaround. Upgrade Node.js to fix properly.\n');
    fileLog('INSECURE_TLS enabled');
  }
  console.log('Press Ctrl+C to stop.');
  await scanHistoryLogBounds();
});

process.on('SIGINT', () => {
  fileLog('---- relay session ended (SIGINT) ----');
  logStream.end(() => process.exit(0));
});
