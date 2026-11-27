const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const {
  getDb, closeDb, searchMedicines, getInventory, addInventoryItem,
  updateInventoryItem, deleteInventoryItem, getDashboardStats,
  getExpiringItems, localDate, CATEGORIES,
} = require('./db/database');

const app = express();
const PORT = parseInt(process.env.PORT, 10) || 3000;
const IS_PROD = process.env.NODE_ENV === 'production';

const QRCode = require('qrcode');
const totp = require('./lib/totp');

// ── Persistent session store (SQLite) ────────────────────────────
// The default MemoryStore logs everybody out on every container restart
// and leaks memory. This keeps sessions in the same database file.
const db = getDb();
db.exec(`CREATE TABLE IF NOT EXISTS sessions (
  sid TEXT PRIMARY KEY, data TEXT NOT NULL, expires INTEGER NOT NULL
)`);

class SqliteStore extends session.Store {
  get(sid, cb) {
    try {
      const row = db.prepare('SELECT data, expires FROM sessions WHERE sid = ?').get(sid);
      if (!row || row.expires < Date.now()) return cb(null, null);
      cb(null, JSON.parse(row.data));
    } catch (e) { cb(e); }
  }
  set(sid, sess, cb) {
    try {
      const expires = sess.cookie && sess.cookie.expires
        ? new Date(sess.cookie.expires).getTime() : Date.now() + 86400000;
      db.prepare('INSERT OR REPLACE INTO sessions (sid, data, expires) VALUES (?, ?, ?)')
        .run(sid, JSON.stringify(sess), expires);
      cb && cb(null);
    } catch (e) { cb && cb(e); }
  }
  destroy(sid, cb) {
    try { db.prepare('DELETE FROM sessions WHERE sid = ?').run(sid); cb && cb(null); }
    catch (e) { cb && cb(e); }
  }
  touch(sid, sess, cb) { this.set(sid, sess, cb); }
}
setInterval(() => {
  try { db.prepare('DELETE FROM sessions WHERE expires < ?').run(Date.now()); } catch (_) {}
}, 60 * 60 * 1000).unref();

// A missing/default SESSION_SECRET is replaced by a random one that is stored
// in the database, so sessions survive restarts and the secret is never guessable.
function resolveSessionSecret() {
  const env = process.env.SESSION_SECRET;
  if (env && env !== 'change-this-to-a-long-random-secret' && env !== 'apohouze-secret-change-this') return env;
  const row = db.prepare("SELECT value FROM app_config WHERE key = 'session_secret'").get();
  if (row) return row.value;
  const secret = crypto.randomBytes(32).toString('hex');
  db.prepare("INSERT INTO app_config (key, value) VALUES ('session_secret', ?)").run(secret);
  console.warn('⚠️  SESSION_SECRET not set — generated a random one and stored it in the database.');
  return secret;
}

// ── Account, setup code and 2FA state (stored in the database) ──
// No passwords live in docker-compose. The account is created in the app on first
// use; it is protected by a one-time setup code that is printed in the container log.
const cfgGet = k => { const r = db.prepare('SELECT value FROM app_config WHERE key = ?').get(k); return r ? JSON.parse(r.value) : null; };
const cfgSet = (k, v) => db.prepare('INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)').run(k, JSON.stringify(v));
const cfgDel = k => db.prepare('DELETE FROM app_config WHERE key = ?').run(k);

const sha = x => crypto.createHash('sha256').update(String(x)).digest();
const safeEqual = (a, b) => crypto.timingSafeEqual(sha(a), sha(b));
const normCode = c => String(c || '').toLowerCase().replace(/[^a-z0-9]/g, '');

if (process.env.RESET_ACCOUNT === 'true') {
  cfgDel('account'); cfgDel('totp'); db.prepare('DELETE FROM sessions').run();
  console.warn('⚠️  RESET_ACCOUNT=true: account and 2FA removed. Remove this variable again after this start.');
}

const getAccount = () => cfgGet('account');
const getTotp = () => cfgGet('totp');
const accountVersion = () => { const a = getAccount(); return a ? a.created : null; };

let setupCode = null;
function ensureSetupCode() {
  if (getAccount()) { setupCode = null; return; }
  const raw = crypto.randomBytes(5).toString('hex');
  setupCode = raw;
  const pretty = `${raw.slice(0, 5)}-${raw.slice(5)}`;
  console.log('\n====================================================');
  console.log('  apoHouze first-time setup');
  console.log(`  Setup code:  ${pretty}`);
  console.log('  Open the app and create your account with this code.');
  console.log('====================================================\n');
}
ensureSetupCode();

// A fixed dummy hash so a wrong username takes as long as a wrong password.
const DUMMY_HASH = bcrypt.hashSync('apohouze-dummy', 12);

const hashRecovery = c => sha(normCode(c)).toString('hex');
function newRecoveryCodes() {
  const plain = Array.from({ length: 8 }, () => {
    const r = crypto.randomBytes(5).toString('hex');
    return `${r.slice(0, 5)}-${r.slice(5)}`;
  });
  return { plain, hashed: plain.map(hashRecovery) };
}

// ── Middleware ───────────────────────────────────────────────────
// Set TRUST_PROXY=1 when running behind nginx / Traefik / NPM with HTTPS.
if (process.env.TRUST_PROXY) {
  const v = process.env.TRUST_PROXY;
  app.set('trust proxy', /^\d+$/.test(v) ? parseInt(v, 10) : v === 'true' ? 1 : v);
}
app.disable('x-powered-by');

app.use((req, res, next) => {
  res.set({
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'same-origin',
    'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
  });
  next();
});

app.use(express.json({ limit: '50kb' }));

app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders(res, file) {
    // Never let the browser keep a stale service worker or HTML shell.
    if (/(sw\.js|index\.html|manifest\.json)$/.test(file)) res.set('Cache-Control', 'no-cache');
  },
}));

app.get('/healthz', (req, res) => {
  try { db.prepare('SELECT 1').get(); res.json({ ok: true }); }
  catch (_) { res.status(503).json({ ok: false }); }
});

app.use(session({
  name: 'apohouze.sid',
  secret: resolveSessionSecret(),
  store: new SqliteStore(),
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    // 'auto' = secure when the request arrived over HTTPS (needs TRUST_PROXY behind a proxy).
    secure: process.env.COOKIE_SECURE === 'true' ? true : process.env.COOKIE_SECURE === 'false' ? false : 'auto',
    maxAge: 7 * 24 * 60 * 60 * 1000,
  },
}));

function requireAuth(req, res, next) {
  const ok = req.session && req.session.authenticated && req.session.acct && req.session.acct === accountVersion();
  if (ok) return next();
  res.status(401).json({ error: 'Not logged in' });
}

// ── Login rate limit: 10 failed attempts / 15 min / IP ───────────
const attempts = new Map();
const WINDOW = 15 * 60 * 1000, MAX_FAILS = 10;
function loginLimiter(req, res, next) {
  const now = Date.now();
  const rec = attempts.get(req.ip);
  if (rec && rec.reset > now && rec.fails >= MAX_FAILS) {
    res.set('Retry-After', Math.ceil((rec.reset - now) / 1000));
    return res.status(429).json({ error: 'Too many attempts. Try again later.' });
  }
  next();
}
function recordFail(ip) {
  const now = Date.now();
  const rec = attempts.get(ip);
  if (!rec || rec.reset <= now) attempts.set(ip, { fails: 1, reset: now + WINDOW });
  else rec.fails++;
}
setInterval(() => {
  const now = Date.now();
  for (const [ip, r] of attempts) if (r.reset <= now) attempts.delete(ip);
}, WINDOW).unref();

// ── Auth ─────────────────────────────────────────────────────────
// stages: create → (no account yet) | login → verify (2FA code) | enroll (set up 2FA) → app
function stageOf(req) {
  const acct = getAccount();
  if (!acct) return 'create';
  const sess = req.session || {};
  if (sess.authenticated && sess.acct === acct.created) return 'ready';
  if (sess.pw && sess.acct === acct.created) return (getTotp() && getTotp().enabled) ? 'verify' : 'enroll';
  return 'login';
}

function startSession(req, res, stage, fn) {
  const acct = getAccount();
  req.session.regenerate(err => {   // fresh session id on every privilege change
    if (err) return res.status(500).json({ error: 'Login failed' });
    req.session.pw = true;
    req.session.acct = acct.created;
    req.session.tries = 0;
    fn && fn(req.session);
    req.session.save(() => res.json({ success: true, stage }));
  });
}

app.get('/api/me', (req, res) => {
  const stage = stageOf(req);
  res.json({ stage, authenticated: stage === 'ready', username: stage === 'ready' ? getAccount().username : undefined });
});

// First use: create the account (needs the one-time code from the container log).
app.post('/api/setup', loginLimiter, (req, res) => {
  if (getAccount()) return res.status(409).json({ error: 'Already set up' });
  const { code, username, password } = req.body || {};
  if (!setupCode || !safeEqual(normCode(code), setupCode)) {
    recordFail(req.ip);
    return res.status(403).json({ error: 'Invalid setup code' });
  }
  const user = String(username || '').trim();
  const pass = String(password || '');
  if (!/^[A-Za-z0-9._@-]{3,40}$/.test(user)) return res.status(400).json({ error: 'Username: 3–40 characters (letters, digits . _ @ -)' });
  if (pass.length < 10 || pass.length > 200) return res.status(400).json({ error: 'Password must be at least 10 characters' });

  db.prepare('DELETE FROM sessions').run();
  cfgSet('account', { username: user, hash: bcrypt.hashSync(pass, 12), created: crypto.randomBytes(8).toString('hex') });
  cfgDel('totp');
  setupCode = null;
  startSession(req, res, 'enroll');
});

app.post('/api/login', loginLimiter, (req, res) => {
  const acct = getAccount();
  if (!acct) return res.status(409).json({ error: 'Setup required', stage: 'create' });
  const { username = '', password = '' } = req.body || {};
  const userOk = safeEqual(username, acct.username);
  const passOk = bcrypt.compareSync(String(password).slice(0, 200), userOk ? acct.hash : DUMMY_HASH);
  if (!(userOk && passOk)) {
    recordFail(req.ip);
    return res.status(401).json({ error: 'Invalid username or password' });
  }
  const t = getTotp();
  startSession(req, res, t && t.enabled ? 'verify' : 'enroll');
});

// Second factor, existing users: a 6-digit app code or a one-time recovery code.
app.post('/api/2fa/verify', loginLimiter, (req, res) => {
  if (stageOf(req) !== 'verify') return res.status(403).json({ error: 'Not allowed' });
  const t = getTotp();
  const input = String((req.body || {}).code || '').trim();
  let ok = false;

  const step = totp.verify(t.secret, input, t.lastStep || 0);
  if (step) { t.lastStep = step; cfgSet('totp', t); ok = true; }
  else {
    const h = hashRecovery(input);
    const idx = (t.recovery || []).indexOf(h);
    if (idx >= 0 && normCode(input).length === 10) { t.recovery.splice(idx, 1); cfgSet('totp', t); ok = true; }
  }

  if (!ok) {
    recordFail(req.ip);
    req.session.tries = (req.session.tries || 0) + 1;
    if (req.session.tries >= 5) return req.session.destroy(() => res.status(401).json({ error: 'Too many attempts', stage: 'login' }));
    return res.status(401).json({ error: 'Invalid code' });
  }
  attempts.delete(req.ip);
  const left = (t.recovery || []).length;
  req.session.regenerate(err => {
    if (err) return res.status(500).json({ error: 'Login failed' });
    req.session.pw = true; req.session.authenticated = true; req.session.acct = getAccount().created;
    req.session.save(() => res.json({ success: true, username: getAccount().username, recoveryLeft: left }));
  });
});

// First-use 2FA enrollment: show secret + QR, then confirm with a code from the app.
app.get('/api/2fa/enroll', async (req, res) => {
  if (stageOf(req) !== 'enroll') return res.status(403).json({ error: 'Not allowed' });
  if (!req.session.pendingSecret) req.session.pendingSecret = totp.generateSecret();
  const secret = req.session.pendingSecret;
  const uri = totp.otpauthUri(secret, getAccount().username);
  try {
    res.json({ secret, uri, qr: await QRCode.toDataURL(uri, { margin: 1, width: 220 }) });
  } catch (e) { fail(res, e); }
});

app.post('/api/2fa/enroll', loginLimiter, (req, res) => {
  if (stageOf(req) !== 'enroll' || !req.session.pendingSecret) return res.status(403).json({ error: 'Not allowed' });
  const secret = req.session.pendingSecret;
  const step = totp.verify(secret, (req.body || {}).code, 0);
  if (!step) {
    recordFail(req.ip);
    req.session.tries = (req.session.tries || 0) + 1;
    if (req.session.tries >= 10) return req.session.destroy(() => res.status(401).json({ error: 'Too many attempts', stage: 'login' }));
    return res.status(401).json({ error: 'Invalid code' });
  }
  attempts.delete(req.ip);
  const rec = newRecoveryCodes();
  cfgSet('totp', { secret, enabled: true, lastStep: step, recovery: rec.hashed });
  delete req.session.pendingSecret;
  req.session.authenticated = true;
  req.session.save(() => res.json({ success: true, username: getAccount().username, recoveryCodes: rec.plain }));
});

app.post('/api/logout', (req, res) => {
  req.session.destroy(() => {
    res.clearCookie('apohouze.sid');
    res.json({ success: true });
  });
});

// ── Helpers ──────────────────────────────────────────────────────
const fail = (res, err, code = 500) => {
  console.error(err);
  res.status(code).json({ error: code === 500 ? 'Internal server error' : err.message });
};

class ValidationError extends Error {}

function cleanItem(body = {}) {
  const str = (v, max) => String(v == null ? '' : v).trim().slice(0, max);
  const name = str(body.name, 200);
  const category = str(body.category, 100);
  if (!name) throw new ValidationError('Name is required');
  if (!category) throw new ValidationError('Category is required');

  let expiry = body.expiry_date ? str(body.expiry_date, 10) : null;
  if (expiry) {
    const d = new Date(expiry + 'T00:00:00Z');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(expiry) || isNaN(d) || d.toISOString().slice(0, 10) !== expiry) {
      throw new ValidationError('Invalid expiry date');
    }
  }

  // parseInt(x) || 1 turned a quantity of 0 ("empty, but keep the box") into 1.
  const q = parseInt(body.quantity, 10);
  const quantity = Number.isFinite(q) ? Math.min(Math.max(q, 0), 100000) : 1;

  return {
    name,
    generic: str(body.generic, 200),
    category,
    form: str(body.form, 100),
    quantity,
    unit: str(body.unit, 30) || 'units',
    expiry_date: expiry,
    notes: str(body.notes, 1000),
    location: str(body.location, 100) || 'Medicine cabinet',
    rx: body.rx ? 1 : 0,
  };
}

const parseId = (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id < 1) { res.status(400).json({ error: 'Invalid id' }); return null; }
  return id;
};

// ── API ──────────────────────────────────────────────────────────
app.get('/api/config', requireAuth, (req, res) => {
  try {
    const s = getDashboardStats();
    res.json({ country: s.country, medicineDbCount: s.medicineDbCount });
  } catch (e) { fail(res, e); }
});

app.get('/api/dashboard', requireAuth, (req, res) => {
  try {
    res.json({ stats: getDashboardStats(), expiring: getExpiringItems(90), today: localDate(0) });
  } catch (e) { fail(res, e); }
});

app.get('/api/medicines/search', requireAuth, (req, res) => {
  const q = String(req.query.q || '');
  if (q.trim().length < 2) return res.json([]);
  try { res.json(searchMedicines(q.slice(0, 100))); } catch (e) { fail(res, e); }
});

app.get('/api/categories', requireAuth, (req, res) => {
  res.json(CATEGORIES.map(c => (c && typeof c === 'object' ? c.name : c)));
});

app.get('/api/inventory', requireAuth, (req, res) => {
  try {
    const { category, search, expiring } = req.query;
    res.json(getInventory({
      category: category ? String(category) : undefined,
      search: search ? String(search).slice(0, 100) : undefined,
      expiring: expiring === '1',
    }));
  } catch (e) { fail(res, e); }
});

app.post('/api/inventory', requireAuth, (req, res) => {
  try {
    res.json({ success: true, id: addInventoryItem(cleanItem(req.body)) });
  } catch (e) { e instanceof ValidationError ? fail(res, e, 400) : fail(res, e); }
});

app.put('/api/inventory/:id', requireAuth, (req, res) => {
  const id = parseId(req, res); if (id === null) return;
  try {
    const r = updateInventoryItem(id, cleanItem(req.body));
    if (!r.changes) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (e) { e instanceof ValidationError ? fail(res, e, 400) : fail(res, e); }
});

app.delete('/api/inventory/:id', requireAuth, (req, res) => {
  const id = parseId(req, res); if (id === null) return;
  try {
    const r = deleteInventoryItem(id);
    if (!r.changes) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (e) { fail(res, e); }
});

app.get('/api/expiring', requireAuth, (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days, 10) || 30, 1), 3650);
    res.json(getExpiringItems(days));
  } catch (e) { fail(res, e); }
});

// When was the medicine list last refreshed? (read fresh: require() would cache it)
app.get('/api/update-status', requireAuth, (req, res) => {
  try {
    const file = path.join(__dirname, 'data', 'last-update.json');
    if (!fs.existsSync(file)) return res.json({ last_update: null, total_added: 0, message: 'No update log found.' });
    res.json(JSON.parse(fs.readFileSync(file, 'utf8')));
  } catch (e) { fail(res, e); }
});

// Unknown API routes must be a JSON 404, not the HTML app shell.
app.use('/api', (req, res) => res.status(404).json({ error: 'Not found' }));

// Malformed JSON bodies etc.
app.use((err, req, res, next) => {
  if (err && err.type === 'entity.parse.failed') return res.status(400).json({ error: 'Invalid JSON' });
  if (err && err.type === 'entity.too.large') return res.status(413).json({ error: 'Payload too large' });
  console.error(err);
  res.status(500).json({ error: 'Internal server error' });
});

// SPA fallback
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── Start / stop ─────────────────────────────────────────────────
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`💊 apoHouze running on port ${PORT} (${IS_PROD ? 'production' : 'development'})`);
});

function shutdown(signal) {
  console.log(`${signal} received, shutting down…`);
  server.close(() => { closeDb(); process.exit(0); });
  setTimeout(() => process.exit(1), 8000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
