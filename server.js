// Siege of Bezzgod server: serves the game page and runs online rooms over WebSockets.
// No dependencies, so Render only needs `node server.js`.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PAGE = path.join(__dirname, 'index.html');

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
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
