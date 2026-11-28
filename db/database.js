const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

// ── Country loader ───────────────────────────────────────────────
// Supported countries are derived from data/countries/*.js, so every country
// listed in the README works (the old hard-coded list silently dropped
// CZ, SK, HR, SI, HU, RO, GR and LU back to BE).
const COUNTRIES_DIR = path.join(__dirname, '..', 'data', 'countries');
const SUPPORTED_COUNTRIES = fs.readdirSync(COUNTRIES_DIR)
  .filter(f => f.endsWith('.js'))
  .map(f => path.basename(f, '.js'));

const requested = (process.env.COUNTRY || 'BE').trim().toLowerCase();
const resolvedCode = SUPPORTED_COUNTRIES.includes(requested) ? requested : 'be';
if (resolvedCode !== requested) {
  console.warn(`⚠️  Unknown COUNTRY '${process.env.COUNTRY}', falling back to BE`);
}

const { MEDICINES, CATEGORIES } = require(path.join(COUNTRIES_DIR, resolvedCode));
console.log(`🌍 Country database loaded: ${resolvedCode.toUpperCase()} — ${MEDICINES.length} medicines`);

const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'apohouze.db');
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

// Accent- and case-insensitive search key: "Dafalgan Forté" → "dafalgan forte"
function fold(s) {
  return String(s || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

// Local calendar date (YYYY-MM-DD). toISOString() is UTC and gives the wrong
// day around midnight for anyone not on UTC.
function localDate(offsetDays = 0) {
  const d = new Date(Date.now() + offsetDays * 86400000);
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

let db;

function getDb() {
  if (!db) {
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('foreign_keys = ON');
    initDb(db);
  }
  return db;
}

function initDb(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS medicine_db (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      generic TEXT,
      category TEXT NOT NULL,
      form TEXT,
      rx INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS inventory (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      generic TEXT,
      category TEXT NOT NULL,
      form TEXT,
      quantity INTEGER DEFAULT 1,
      unit TEXT DEFAULT 'units',
      expiry_date DATE,
      notes TEXT,
      location TEXT DEFAULT 'Medicine cabinet',
      rx INTEGER DEFAULT 0,
      added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    -- Pack code (GTIN or raw barcode) -> medicine, learned the first time a pack is scanned.
    CREATE TABLE IF NOT EXISTS product_codes (
      code TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      generic TEXT,
      category TEXT NOT NULL,
      form TEXT,
      unit TEXT DEFAULT 'units',
      rx INTEGER DEFAULT 0,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS app_config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_inventory_expiry   ON inventory(expiry_date);
    CREATE INDEX IF NOT EXISTS idx_inventory_category ON inventory(category);
  `);

  // Add the folded search column on databases created by older versions.
  const cols = db.prepare('PRAGMA table_info(medicine_db)').all().map(c => c.name);
  if (!cols.includes('search')) db.exec('ALTER TABLE medicine_db ADD COLUMN search TEXT');
  db.exec('CREATE INDEX IF NOT EXISTS idx_medicine_search ON medicine_db(search)');

  // Reload when the country, the entries or the schema version changed.
  // (Count alone missed edits that kept the same number of medicines.)
  const SCHEMA = '3';
  const fingerprint = `${resolvedCode}:${MEDICINES.length}:${fold(MEDICINES[0] && MEDICINES[0].name)}:${fold(MEDICINES[MEDICINES.length - 1] && MEDICINES[MEDICINES.length - 1].name)}:v${SCHEMA}`;
  const stored = db.prepare("SELECT value FROM app_config WHERE key = 'fingerprint'").get();

  if (!stored || stored.value !== fingerprint) {
    console.log(`🔄 Reloading medicine DB: ${resolvedCode.toUpperCase()} (${MEDICINES.length} medicines)`);
    const insert = db.prepare(
      'INSERT INTO medicine_db (name, generic, category, form, rx, search) VALUES (?, ?, ?, ?, ?, ?)'
    );
    db.transaction(() => {
      db.prepare('DELETE FROM medicine_db').run();
      for (const m of MEDICINES) {
        insert.run(m.name, m.generic || '', m.category, m.form || '', m.rx ? 1 : 0,
          fold(`${m.name} ${m.generic || ''}`));
      }
      db.prepare("INSERT OR REPLACE INTO app_config (key, value) VALUES ('fingerprint', ?)").run(fingerprint);
      db.prepare("INSERT OR REPLACE INTO app_config (key, value) VALUES ('country', ?)").run(resolvedCode);
    })();
    console.log('✅ Medicine database synced');
  } else {
    console.log('✅ Medicine database up to date');
  }
}

// Escape LIKE wildcards so searching for "50%" or "_" behaves literally.
const likeEscape = s => s.replace(/[\\%_]/g, c => '\\' + c);

function searchMedicines(query, limit = 15) {
  const q = fold(query).trim();
  if (!q) return [];
  const esc = likeEscape(q);
  return getDb().prepare(`
    SELECT name, generic, category, form, rx
    FROM medicine_db
    WHERE search LIKE ? ESCAPE '\\'
    ORDER BY
      CASE WHEN search LIKE ? ESCAPE '\\' THEN 0 ELSE 1 END,
      length(name),
      name
    LIMIT ?
  `).all(`%${esc}%`, `${esc}%`, limit);
}

function getInventory(filters = {}) {
  let sql = 'SELECT * FROM inventory WHERE 1=1';
  const params = [];

  if (filters.category) {
    sql += ' AND category = ?';
    params.push(filters.category);
  }
  if (filters.search) {
    sql += " AND (name LIKE ? ESCAPE '\\' OR generic LIKE ? ESCAPE '\\' OR notes LIKE ? ESCAPE '\\')";
    const s = `%${likeEscape(filters.search)}%`;
    params.push(s, s, s);
  }
  if (filters.expiring) {
    // The old query used date("now", ...) with double quotes, which
    // better-sqlite3 (compiled with SQLITE_DQS=0) rejects as an unknown column.
    sql += ' AND expiry_date IS NOT NULL AND expiry_date <= ?';
    params.push(localDate(30));
  }

  sql += ' ORDER BY expiry_date IS NULL, expiry_date ASC, name ASC';
  return getDb().prepare(sql).all(...params);
}

function addInventoryItem(item) {
  return getDb().prepare(`
    INSERT INTO inventory (name, generic, category, form, quantity, unit, expiry_date, notes, location, rx)
    VALUES (@name, @generic, @category, @form, @quantity, @unit, @expiry_date, @notes, @location, @rx)
  `).run(item).lastInsertRowid;
}

function updateInventoryItem(id, item) {
  return getDb().prepare(`
    UPDATE inventory SET
      name = @name, generic = @generic, category = @category, form = @form,
      quantity = @quantity, unit = @unit, expiry_date = @expiry_date,
      notes = @notes, location = @location, rx = @rx,
      updated_at = CURRENT_TIMESTAMP
    WHERE id = @id
  `).run({ ...item, id });
}

function getProductCode(code) {
  return getDb().prepare(
    'SELECT code, name, generic, category, form, unit, rx FROM product_codes WHERE code = ?'
  ).get(code) || null;
}

function saveProductCode(code, item) {
  getDb().prepare(`
    INSERT INTO product_codes (code, name, generic, category, form, unit, rx, updated_at)
    VALUES (@code, @name, @generic, @category, @form, @unit, @rx, CURRENT_TIMESTAMP)
    ON CONFLICT(code) DO UPDATE SET
      name = excluded.name, generic = excluded.generic, category = excluded.category,
      form = excluded.form, unit = excluded.unit, rx = excluded.rx,
      updated_at = CURRENT_TIMESTAMP
  `).run({
    code, name: item.name, generic: item.generic || '', category: item.category,
    form: item.form || '', unit: item.unit || 'units', rx: item.rx ? 1 : 0,
  });
}

function deleteInventoryItem(id) {
  return getDb().prepare('DELETE FROM inventory WHERE id = ?').run(id);
}

function getDashboardStats() {
  const db = getDb();
  const today = localDate(0), in30 = localDate(30), in90 = localDate(90);
  const count = (sql, ...p) => db.prepare(sql).get(...p).c;
  return {
    total: count('SELECT COUNT(*) c FROM inventory'),
    expired: count('SELECT COUNT(*) c FROM inventory WHERE expiry_date IS NOT NULL AND expiry_date < ?', today),
    expiring30: count('SELECT COUNT(*) c FROM inventory WHERE expiry_date >= ? AND expiry_date <= ?', today, in30),
    expiring90: count('SELECT COUNT(*) c FROM inventory WHERE expiry_date > ? AND expiry_date <= ?', in30, in90),
    categories: count('SELECT COUNT(DISTINCT category) c FROM inventory'),
    rx: count('SELECT COUNT(*) c FROM inventory WHERE rx = 1'),
    country: resolvedCode.toUpperCase(),
    medicineDbCount: MEDICINES.length,
  };
}

function getExpiringItems(days = 30) {
  return getDb().prepare(`
    SELECT * FROM inventory
    WHERE expiry_date IS NOT NULL AND expiry_date <= ?
    ORDER BY expiry_date ASC
  `).all(localDate(days));
}

function closeDb() { if (db) { db.close(); db = null; } }

module.exports = {
  getDb, closeDb, searchMedicines, getInventory, addInventoryItem,
  updateInventoryItem, deleteInventoryItem, getDashboardStats, getExpiringItems,
  getProductCode, saveProductCode,
  localDate, CATEGORIES, SUPPORTED_COUNTRIES,
};
