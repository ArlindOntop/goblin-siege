// Siege of Bezzgod server: serves the game page and runs online rooms over WebSockets.
// No dependencies, so Render only needs `node server.js`.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PAGE = path.join(__dirname, 'index.html');

/* ---------- Endless leaderboard ----------
   Saved in Upstash Redis when UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN are set (survives restarts).
   Otherwise saved to a local file, which Render wipes on every restart or redeploy. */
const LB_KEY = 'sob-leaderboard-endless';
const LB_FILE = path.join(__dirname, 'leaderboard.json');
const LB_MAX = 100;
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, '');
const UP_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const lbPersistent = !!(UP_URL && UP_TOKEN);
let lbCache = null;
async function lbLoad() {
  if (lbCache) return lbCache;
  try {
    if (lbPersistent) {
      const r = await fetch(UP_URL + '/get/' + LB_KEY, { headers: { Authorization: 'Bearer ' + UP_TOKEN } });
      const j = await r.json();
      lbCache = j.result ? JSON.parse(j.result) : [];
    } else {
      lbCache = fs.existsSync(LB_FILE) ? JSON.parse(fs.readFileSync(LB_FILE, 'utf8')) : [];
    }
  } catch (e) { console.error('leaderboard load failed', e.message); lbCache = lbCache || []; }
  if (!Array.isArray(lbCache)) lbCache = [];
  return lbCache;
}
async function lbSave(list) {
  lbCache = list;
  try {
    if (lbPersistent) await fetch(UP_URL + '/set/' + LB_KEY, { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOKEN }, body: JSON.stringify(list) });
    else fs.writeFileSync(LB_FILE, JSON.stringify(list));
  } catch (e) { console.error('leaderboard save failed', e.message); }
}
const lastPost = new Map();
function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > limit) { reject(new Error('too big')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
function json(res, code, obj) { res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); }
async function handleApi(req, res, url) {
  if (url === '/api/leaderboard' && req.method === 'GET') {
    const list = await lbLoad();
    json(res, 200, { entries: list.slice(0, 25), persistent: lbPersistent });
    return;
  }
  if (url === '/api/score' && req.method === 'POST') {
    const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    const now = Date.now();
    if (now - (lastPost.get(ip) || 0) < 2500) { json(res, 429, { error: 'Slow down a little, then try again.' }); return; }
    lastPost.set(ip, now);
    let b; try { b = JSON.parse(await readBody(req, 4096)); } catch (e) { json(res, 400, { error: 'Bad request' }); return; }
    const wave = Math.floor(Number(b.wave));
    if (!(wave >= 1 && wave <= 2000)) { json(res, 400, { error: 'That wave number is not valid.' }); return; }
    const name = cleanName(b.name).slice(0, 40);
    const players = Array.isArray(b.players) ? b.players.slice(0, 4).map(cleanName) : [name];
    const map = String(b.map || '').replace(/[^\w ]/g, '').slice(0, 30);
    const entry = { name, players, wave, map, date: new Date().toISOString().slice(0, 10), id: crypto.randomBytes(4).toString('hex') };
    const list = (await lbLoad()).slice();
    list.push(entry);
    list.sort((a, b) => b.wave - a.wave || a.date.localeCompare(b.date));
    const rank = list.indexOf(entry) + 1;
    await lbSave(list.slice(0, LB_MAX));
    json(res, 200, { rank, id: entry.id, entries: list.slice(0, 25) });
    return;
  }
  json(res, 404, { error: 'Not found' });
}

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  if (url.startsWith('/api/')) { handleApi(req, res, url).catch(() => json(res, 500, { error: 'Server error' })); return; }
  if (url === '/' || url === '/index.html') {
    fs.readFile(PAGE, (err, data) => {
      if (err) { res.writeHead(500); res.end('Game file missing'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(302, { Location: '/' }); res.end();
});

/* ---------- minimal WebSocket (RFC 6455) ---------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MSG = 1 << 20;

class Conn {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.frag = []; this.closed = false; this.alive = true;
    this.room = null; this.pid = 0; this.name = ''; this.isGuest = false;
    sock.on('data', d => this.onData(d));
    sock.on('close', () => this.onClose());
    sock.on('error', () => this.onClose());
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    while (this.buf.length >= 2) {
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_MSG) { this.sock.destroy(); return; }
      const maskAt = off; if (masked) off += 4;
      if (this.buf.length < off + len) return;
      let payload = Buffer.from(this.buf.subarray(off, off + len));
      if (masked) { const m = this.buf.subarray(maskAt, maskAt + 4); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      this.buf = this.buf.subarray(off + len);
      if (op === 8) { this.close(); return; }
      if (op === 9) { this.sendRaw(10, payload); continue; }
      if (op === 10) { this.alive = true; continue; }
      if (op === 0 || op === 1 || op === 2) {
        this.frag.push(payload);
        if (fin) { const msg = Buffer.concat(this.frag).toString('utf8'); this.frag = []; this.onMessage(msg); }
      }
    }
  }
  sendRaw(op, payload) {
    if (this.closed || this.sock.destroyed) return;
    const len = payload.length; let h;
    if (len < 126) h = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
    this.sock.write(Buffer.concat([h, payload]));
  }
  send(obj) { this.sendRaw(1, Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj))); }
  close() { if (this.closed) return; this.sendRaw(8, Buffer.alloc(0)); this.sock.end(); this.onClose(); }
  onMessage(raw) { let m; try { m = JSON.parse(raw); } catch (e) { return; } handle(this, m); }
  onClose() {
    if (this.closed) return; this.closed = true;
    conns.delete(this);
    const r = this.room; this.room = null; if (!r) return;
    if (r.host === this) {
      for (const p of r.peers.values()) { p.send({ type: 'host-left' }); p.room = null; }
      rooms.delete(r.code);
    } else {
      r.peers.delete(this.pid);
      r.host.send({ type: 'peer-left', pid: this.pid });
    }
  }
}

/* ---------- rooms ---------- */
const rooms = new Map();
const conns = new Set();
let nextPid = 1;
const LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
function newCode() { let c; do { c = Array.from({ length: 4 }, () => LETTERS[Math.floor(Math.random() * LETTERS.length)]).join(''); } while (rooms.has(c)); return c; }
const cleanName = n => String(n || 'Player').replace(/[\u0000-\u001f]/g, '').trim().slice(0, 14) || 'Player';

function handle(c, m) {
  switch (m.type) {
    case 'create': {
      if (c.room) return;
      c.pid = nextPid++; c.name = cleanName(m.name);
      const code = newCode();
      c.room = { code, host: c, peers: new Map() };
      rooms.set(code, c.room);
      c.send({ type: 'created', code, pid: c.pid });
      break;
    }
    case 'join': {
      if (c.room) return;
      const r = rooms.get(String(m.code || '').toUpperCase());
      if (!r) { c.send({ type: 'error', msg: 'There is no room with that code. Check the letters and try again.' }); return; }
      if (r.peers.size >= 3) { c.send({ type: 'error', msg: 'That room is full. Rooms hold 4 players.' }); return; }
      c.pid = nextPid++; c.name = cleanName(m.name); c.room = r; c.isGuest = true;
      r.peers.set(c.pid, c);
      c.send({ type: 'joined', code: r.code, pid: c.pid });
      r.host.send({ type: 'peer-join', pid: c.pid, name: c.name });
      break;
    }
    case 'to-host':
      if (c.isGuest && c.room) c.room.host.send({ type: 'from-peer', pid: c.pid, data: m.data });
      break;
    case 'broadcast':
      if (c.room && c.room.host === c) {
        const s = JSON.stringify({ type: 'msg', data: m.data });
        for (const p of c.room.peers.values()) p.send(s);
      }
      break;
  }
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  conns.add(new Conn(sock));
});

// keep connections alive through Render's proxy, and drop dead ones
setInterval(() => {
  for (const c of conns) {
    if (!c.alive) { c.sock.destroy(); c.onClose(); continue; }
    c.alive = false; c.sendRaw(9, Buffer.alloc(0));
  }
}, 25000);

server.listen(PORT, () => console.log('Siege of Bezzgod running on port ' + PORT));
