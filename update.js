#!/usr/bin/env node
/**
 * apoHouze — Medicine Database Updater v5
 * ========================================
 * BE: medicinesdatabase.be (FAMHP) — officiële Belgische database, dagelijks bijgewerkt
 * NL: CBG Geneesmiddeleninformatiebank — wekelijks bijgewerkt
 */
'use strict';
const fs    = require('fs');
const path  = require('path');
const https = require('https');
const http  = require('http');

const DATA_DIR = path.join(__dirname, 'data', 'countries');
const LOG_FILE = path.join(__dirname, 'data', 'last-update.json');
const TMP_DIR  = path.join(__dirname, 'data', '_tmp');
if (!fs.existsSync(TMP_DIR)) fs.mkdirSync(TMP_DIR, { recursive: true });

const DRY_RUN = process.argv.includes('--dry-run');
const args    = process.argv.slice(2).filter(a => !a.startsWith('--'));
const targets = args.length ? args.map(a => a.toLowerCase()) : ['be', 'nl'];

// ================================================================
// ATC-CODE MAPPING
// ================================================================
const ATC_MAP = {
  A02:'Stomach & Intestine', A03:'Stomach & Intestine', A04:'Stomach & Intestine',
  A05:'Stomach & Intestine', A06:'Stomach & Intestine', A07:'Stomach & Intestine',
  A08:'Stomach & Intestine', A09:'Stomach & Intestine', A10:'Diabetes',
  A11:'Vitamins & Supplements', A12:'Vitamins & Supplements', A13:'Vitamins & Supplements',
  A16:'Stomach & Intestine',
  B01:'Anticoagulants', B02:'Heart & Blood Pressure', B03:'Vitamins & Supplements',
  B05:'Heart & Blood Pressure', B06:'Heart & Blood Pressure',
  C01:'Heart & Blood Pressure', C02:'Heart & Blood Pressure', C03:'Heart & Blood Pressure',
  C04:'Heart & Blood Pressure', C05:'Heart & Blood Pressure', C07:'Heart & Blood Pressure',
  C08:'Heart & Blood Pressure', C09:'Heart & Blood Pressure', C10:'Cholesterol',
  D01:'Antifungals', D02:'Skin & Wounds', D03:'Skin & Wounds', D04:'Skin & Wounds',
  D05:'Skin & Wounds', D06:'Antibiotics', D07:'Corticosteroids', D08:'Skin & Wounds',
  D09:'Skin & Wounds', D10:'Skin & Wounds', D11:'Skin & Wounds',
  G01:"Women's Health", G02:"Women's Health", G03:"Women's Health", G04:'Urology',
  H01:'Thyroid', H02:'Corticosteroids', H03:'Thyroid', H04:'Diabetes',
  H05:'Vitamins & Supplements',
  J01:'Antibiotics', J02:'Antifungals', J04:'Antibiotics', J05:'Antivirals',
  J06:'Antivirals', J07:'Antivirals',
  L01:'Oncology', L02:'Oncology', L03:'Oncology', L04:'Corticosteroids',
  M01:'Pain & Fever', M02:'Joints & Muscles', M03:'Joints & Muscles',
  M04:'Joints & Muscles', M05:'Joints & Muscles', M09:'Joints & Muscles',
  N01:'Pain & Fever', N02:'Pain & Fever', N03:'Neurology', N04:'Neurology',
  N05:'Sleep & Sedation', N06:'Antidepressants', N07:'Nervous System',
  P01:'Antiparasitics', P02:'Antiparasitics', P03:'Antiparasitics',
  R01:'Cough & Cold', R02:'Cough & Cold', R03:'Lungs & Asthma',
  R04:'Cough & Cold', R05:'Cough & Cold', R06:'Allergy', R07:'Lungs & Asthma',
  S01:'Eye & Ear', S02:'Eye & Ear', S03:'Eye & Ear',
  V03:'First Aid', V06:'Vitamins & Supplements', V07:'First Aid', V08:'First Aid',
};
function atcToCategory(atc) {
  if (!atc) return null;
  return ATC_MAP[atc.trim().substring(0, 3).toUpperCase()] || null;
}

// ================================================================
// VORM MAPPING
// ================================================================
const FORM_MAP = [
  [/bruistablet|effervesc/i,              'Effervescent tablet'],
  [/smelttablet|orodispers|dispergeer/i,  'Dispersible tablet'],
  [/oogdruppels|collyre|eye.?drop/i,      'Eye drops'],
  [/oordruppels|otic|ear.?drop/i,         'Ear drops'],
  [/neusspray|nasal.?spray|spray.?nasal/i,'Nasal spray'],
  [/inhalator|inhaler|aerosol|poeder.*inhal/i,'Inhaler'],
  [/tablet|tabl\b|tablette/i,             'Tablet'],
  [/capsule|cap\b|capsul/i,               'Capsule'],
  [/siroop|sirop|syrup|drank/i,           'Syrup'],
  [/druppels|drops|gouttes/i,             'Drops'],
  [/crème|cream|creme/i,                  'Cream'],
  [/zalf|ointment|pommade/i,              'Ointment'],
  [/gel\b/i,                              'Gel'],
  [/pleister|patch|transderm/i,           'Patch'],
  [/spray\b/i,                            'Spray'],
  [/inject|infuus|infusion/i,             'Injection'],
  [/zetpil|suppositoire|suppos/i,         'Suppository'],
  [/poeder|powder|poudre/i,               'Powder'],
  [/suspensie|suspension/i,               'Suspension'],
  [/oplossing|solution/i,                 'Solution'],
  [/mondwater|mouthwash/i,                'Mouthwash'],
  [/kauwgom|chewing.?gum/i,              'Chewing gum'],
  [/zuigtablet|pastille|lozenge/i,        'Lozenge'],
  [/klysma|enema/i,                       'Enema'],
  [/ampul|ampoule/i,                      'Ampoule'],
];
function mapForm(text) {
  if (!text) return 'Tablet';
  for (const [re, form] of FORM_MAP) if (re.test(text)) return form;
  return 'Tablet';
}

// ================================================================
// HELPERS
// ================================================================
function curlDownload(url, dest, maxTime = 120) {
  const { execSync } = require('child_process');
  execSync(
    `curl -L --max-time ${maxTime} --connect-timeout 15 --silent --fail ` +
    `--user-agent "Mozilla/5.0 apoHouze-updater/5.0" ` +
    `-o "${dest}" "${url}"`,
    { timeout: (maxTime + 10) * 1000 }
  );
  return fs.existsSync(dest) ? fs.statSync(dest).size : 0;
}

function loadExistingNames(code) {
  const fp = path.join(DATA_DIR, `${code}.js`);
  if (!fs.existsSync(fp)) return null;
  const content = fs.readFileSync(fp, 'utf8');
  const names = new Set();
  for (const m of content.matchAll(/name:\s*"([^"]+)"/g))
    names.add(m[1].toLowerCase().trim());
  return { content, names };
}

function appendMedicines(code, medicines) {
  if (!medicines.length) return 0;
  const fp = path.join(DATA_DIR, `${code}.js`);
  let content = fs.readFileSync(fp, 'utf8');
  const insertAt = content.indexOf('\n];');
  if (insertAt === -1) return 0;
  const lines = medicines.map(m =>
    `  { name: ${JSON.stringify(m.name)}, generic: ${JSON.stringify(m.generic||'')}, ` +
    `category: ${JSON.stringify(m.category)}, form: ${JSON.stringify(m.form)}, rx: ${m.rx} },`
  ).join('\n');
  const updated = content.slice(0, insertAt) + '\n' + lines + '\n' + content.slice(insertAt);
  if (!DRY_RUN) fs.writeFileSync(fp, updated, 'utf8');
  return medicines.length;
}

function processRows(rows, country, code) {
  if (!rows.length) return 0;
  const sample = rows[0];
  const keys = Object.keys(sample).map(k => k.toLowerCase());

  const findKey = (...patterns) => {
    const orig = Object.keys(sample);
    for (const k of orig) {
      if (patterns.some(p => p.test(k.toLowerCase()))) return k;
    }
    return null;
  };

  const nameKey   = findKey(/^naam$/, /^productnaam$/, /^name$/, /^product/);
  const innKey    = findKey(/werkzame/, /inn/, /actieve/, /generic/, /substance/);
  const atcKey    = findKey(/^atc/);
  const formKey   = findKey(/farmaceutische/, /^vorm$/, /pharmaceutical/, /toedien/);
  const rxKey     = findKey(/afleverstatus/, /recept/, /^rx$/, /prescri/, /\bura\b/);
  const statusKey = findKey(/status/, /vergunn/, /autoris/);

  console.log(`  📋 name:${nameKey} inn:${innKey} atc:${atcKey} form:${formKey} rx:${rxKey}`);

  if (!nameKey) {
    console.error('  ❌ Naamkolom niet gevonden. Beschikbare kolommen:', Object.keys(sample).join(', '));
    return 0;
  }

  const newMeds = [];
  const seen = new Set(country.names);
  let skippedStatus = 0, skippedAtc = 0, skippedExists = 0;

  for (const row of rows) {
    if (statusKey) {
      const s = String(row[statusKey] || '').toLowerCase();
      if (/ingetrokk|geweigerd|geschorst|suspend|revoked|refused|withdrawn/i.test(s)) {
        skippedStatus++; continue;
      }
    }

    const name = String(row[nameKey] || '').trim();
    if (!name) continue;
    if (seen.has(name.toLowerCase())) { skippedExists++; continue; }

    const atc     = atcKey   ? String(row[atcKey]   || '').trim() : '';
    const inn     = innKey   ? String(row[innKey]   || '').trim() : '';
    const formRaw = formKey  ? String(row[formKey]  || '').trim() : '';
    const rxRaw   = rxKey    ? String(row[rxKey]    || '').trim() : '';

    const category = atcToCategory(atc);
    if (!category) { skippedAtc++; continue; }

    const form = mapForm(formRaw);
    const rx   = /\bUA\b|\bURA\b|recept|prescri/i.test(rxRaw);

    newMeds.push({ name, generic: inn, category, form, rx });
    seen.add(name.toLowerCase());
  }

  console.log(`  📊 Nieuw: ${newMeds.length} | Bestond al: ${skippedExists} | Geen ATC: ${skippedAtc} | Ingetrokken: ${skippedStatus}`);
  return appendMedicines(code, newMeds);
}

function parseFile(filePath, code, country) {
  let content = fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, '');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length < 2) { console.error('  ❌ Leeg bestand'); return 0; }

  // Detecteer het scheidingsteken
  const firstLine = lines[0];
  let sep = firstLine.includes('|') ? '|' : firstLine.includes('\t') ? '\t' : firstLine.includes(';') ? ';' : ',';
  const headers = firstLine.split(sep).map(h => h.replace(/"/g, '').trim());
  console.log(`  📄 CSV (${sep}): ${lines.length} rijen, kolommen: ${headers.slice(0,6).join(', ')}`);

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(sep).map(c => c.replace(/^"|"$/g, '').trim());
    const row = {};
    headers.forEach((h, idx) => { row[h] = cols[idx] || ''; });
    rows.push(row);
  }
  return processRows(rows, country, code);
}

// ================================================================
// BELGIË — medicinesdatabase.be (FAMHP)
// Officiële database van alle vergunde Belgische medicijnen.
// Dagelijks bijgewerkt, geen authenticatie nodig.
// ================================================================
async function updateBE() {
  console.log('\n🇧🇪 België — medicinesdatabase.be (FAMHP) ophalen...');
  const country = loadExistingNames('be');
  if (!country) { console.error('  ❌ be.js niet gevonden'); return 0; }

  const dest = path.join(TMP_DIR, 'be_medicines.bin');
  const url = 'https://medicinesdatabase.be/download/human/medicines';

  try {
    console.log(`  📥 Downloaden...`);
    const size = curlDownload(url, dest);
    if (size < 1000) throw new Error(`Bestand te klein: ${size} bytes`);
    console.log(`  ✅ Gedownload: ${(size/1024).toFixed(0)} KB`);
  } catch (e) {
    console.error(`  ❌ Download mislukt: ${e.message}`);
    return 0;
  }

  return parseFile(dest, 'be', country);
}

// ================================================================
// NEDERLAND — CBG Geneesmiddeleninformatiebank
// ================================================================
async function updateNL() {
  console.log('\n🇳🇱 Nederland — CBG Geneesmiddeleninformatiebank ophalen...');
  const country = loadExistingNames('nl');
  if (!country) { console.error('  ❌ nl.js niet gevonden'); return 0; }

  const dest = path.join(TMP_DIR, 'nl_medicines.bin');
  const url = 'https://www.geneesmiddeleninformatiebank.nl/metadata.csv';

  try {
    console.log(`  📥 Downloaden van geneesmiddeleninformatiebank.nl...`);
    const size = curlDownload(url, dest);
    if (size < 10000) throw new Error(`Bestand te klein: ${size} bytes`);
    console.log(`  ✅ Gedownload: ${(size/1024).toFixed(0)} KB`);
  } catch (e) {
    console.error(`  ❌ Download mislukt: ${e.message}`);
    return 0;
  }

  return parseFile(dest, 'nl', country);
}

// ================================================================
// DUITSLAND — EMA + BfArM via Python fetch script
// Duitsland heeft geen centrale publieke CSV-download zoals BE/NL.
// Het Python script fetch_de_medicines.py combineert:
//   1. EMA (gecentraliseerde EU-vergunningen, heeft ATC-codes)
//   2. BfArM AMIS (nationale vergunningen, indien beschikbaar)
// Output: data/_tmp/de_medicines.csv → zelfde parseFile() flow als BE/NL
// ================================================================
async function updateDE() {
  console.log('\n🇩🇪 Duitsland — EMA + BfArM ophalen via Python script...');
  const country = loadExistingNames('de');
  if (!country) { console.error('  ❌ de.js niet gevonden'); return 0; }

  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_de_medicines.py');

  if (!fs.existsSync(script)) {
    console.error('  ❌ fetch_de_medicines.py niet gevonden');
    return 0;
  }

  // Controleer of python3 beschikbaar is
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }

  // Installeer openpyxl indien nodig
  try {
    execSync(`${python} -c "import openpyxl"`, { stdio: 'ignore' });
  } catch {
    console.log('  📦 openpyxl installeren...');
    try { execSync(`${python} -m pip install openpyxl --quiet`, { stdio: 'pipe' }); }
    catch { console.log('  ⚠️  openpyxl installatie mislukt, pandas als fallback...'); }
  }

  console.log('  🐍 Python fetch script uitvoeren...');
  try {
    execSync(`${python} "${script}"`, {
      stdio: 'inherit',
      timeout: 300_000,
      cwd: __dirname,
    });
  } catch (e) {
    console.error(`  ❌ Python script mislukt: ${e.message}`);
    return 0;
  }

  const dest = path.join(TMP_DIR, 'de_medicines.csv');
  if (!fs.existsSync(dest)) {
    console.error('  ❌ de_medicines.csv niet aangemaakt door script');
    return 0;
  }

  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size} bytes`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);

  return parseFile(dest, 'de', country);
}

// ================================================================
// VERENIGD KONINKRIJK — MHRA Product Information Database
// MHRA publiceert een publieke CSV van alle vergunde producten.
// URL: https://products.mhra.gov.uk/downloads/
// Bestand: products.csv (~50MB, geen authenticatie)
// ================================================================
async function updateGB() {
  console.log('\n🇬🇧 Verenigd Koninkrijk — MHRA Products Database ophalen...');
  const country = loadExistingNames('gb');
  if (!country) { console.error('  ❌ gb.js niet gevonden'); return 0; }

  const dest = path.join(TMP_DIR, 'gb_medicines.csv');

  // MHRA publiceert products.csv als open data
  // Kolomstructuur: Product Name, Active Ingredient, ATC Code, Pharmaceutical Form, Legal Category
  const urls = [
    'https://products.mhra.gov.uk/api/download/csv',
    'https://products.mhra.gov.uk/downloads/products.csv',
    'https://www.gov.uk/guidance/product-licences-pls/products-csv',
  ];

  let downloaded = false;
  for (const url of urls) {
    try {
      console.log(`  📥 Proberen: ${url}`);
      const size = curlDownload(url, dest, 180);
      if (size > 10000) {
        console.log(`  ✅ Gedownload: ${(size/1024).toFixed(0)} KB`);
        downloaded = true;
        break;
      }
    } catch (e) {
      console.log(`  ⚠️  URL mislukt: ${e.message}`);
    }
  }

  if (!downloaded) {
    console.error('  ❌ MHRA download mislukt — alle URLs geprobeerd');
    return 0;
  }

  return parseFile(dest, 'gb', country);
}

// ================================================================
// HOOFD
// ================================================================
async function main() {
  console.log('\n🔄 apoHouze Medicine Database Updater v5');
  console.log(`📅 ${new Date().toISOString()}`);
  if (DRY_RUN) console.log('🔍 DRY RUN — geen bestanden worden gewijzigd');

  const log = { updated_at: new Date().toISOString(), dry_run: DRY_RUN, results: {} };
  let totalAdded = 0;

  for (const target of targets) {
    const before = loadExistingNames(target)?.names.size || 0;
    let added = 0;

    if (target === 'be') added = await updateBE();
    else if (target === 'nl') added = await updateNL();
    else if (target === 'de') added = await updateDE();
    else if (target === 'gb') added = await updateGB();
    else { console.log(`⚠️  Onbekend land: ${target} (ondersteund: be, nl, de, gb)`); continue; }

    const after = loadExistingNames(target)?.names.size || 0;
    log.results[target] = { before, after, added: after - before };
    totalAdded += (after - before);
    console.log(`  ✅ ${target.toUpperCase()}: ${before} → ${after} medicijnen (+${after - before} nieuw)\n`);
  }

  try { require('child_process').execSync(`rm -rf "${TMP_DIR}"`); } catch {}

  if (!DRY_RUN) fs.writeFileSync(LOG_FILE, JSON.stringify(log, null, 2));

  console.log(`🎉 Klaar! Totaal toegevoegd: ${totalAdded} nieuwe medicijnen`);
  if (totalAdded > 0 && !DRY_RUN) console.log('🚀 Commit en push om Docker rebuild te triggeren.');
  process.exit(0);
}

main().catch(err => { console.error('❌ Fout:', err.message); process.exit(1); });
