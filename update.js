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

// Naam-gebaseerde keyword fallback voor landen zonder ATC (BE-OTC, FR zonder ATC, etc.)
function guessCategory(name) {
  if (!name) return null;
  const n = name.toLowerCase();
  if (/paracetamol|ibuprofen|aspirin|naprox|diclofenac|ketoprofen|tramadol|codeine|oxycodon|fentanyl|morphine/.test(n)) return 'Pain & Fever';
  if (/amoxicillin|azithromycin|ciprofloxacin|doxycyclin|metronidazol|clindamycin|cephalexin|nitrofurantoin|levofloxacin|clarithromycin|penicillin/.test(n)) return 'Antibiotics';
  if (/aciclovir|valaciclovir|oseltamivir|tenofovir|emtricitabin|lopinavir|ritonavir|dolutegravir/.test(n)) return 'Antivirals';
  if (/fluconazol|clotrimazol|miconazol|terbinafin|nystatin|amphotericin|ketoconazol/.test(n)) return 'Antifungals';
  if (/loratadin|cetirizin|fexofenadine|levocetirizin|desloratadin|diphenhydramin|hydroxyzine|chlorpheniramin/.test(n)) return 'Allergy';
  if (/dextromethorphan|guaifenesin|pseudoephedrin|phenylephrin|oxymetazolin|xylometazolin|bromhexin|ambroxol|acetylcysteine/.test(n)) return 'Cough & Cold';
  if (/salbutamol|albuterol|salmeterol|formoterol|tiotropium|budesonide|fluticason|montelukast|theophyllin/.test(n)) return 'Lungs & Asthma';
  if (/omeprazol|pantoprazol|esomeprazol|lansoprazol|rabeprazol|famotidin|ranitidine|loperamide|ondansetron|metoclopramide|domperidon/.test(n)) return 'Stomach & Intestine';
  if (/amlodipine|lisinopril|losartan|metoprolol|atenolol|hydrochlorothiazid|furosemid|spironolacton|enalapril|ramipril|carvedilol|bisoprolol|valsartan|diltiazem|verapamil/.test(n)) return 'Heart & Blood Pressure';
  if (/atorvastatin|simvastatin|rosuvastatin|pravastatin|ezetimibe|fenofibrat/.test(n)) return 'Cholesterol';
  if (/warfarin|apixaban|rivaroxaban|clopidogrel|enoxaparin|dabigatran|heparin|ticagrelor/.test(n)) return 'Anticoagulants';
  if (/metformin|insulin|empagliflozin|semaglutide|liraglutide|sitagliptin|glipizide|gliclazid/.test(n)) return 'Diabetes';
  if (/levothyroxin|methimazol|propylthiouracil/.test(n)) return 'Thyroid';
  if (/prednison|prednisolon|methylprednisolon|dexamethason|hydrocortison|betamethason/.test(n)) return 'Corticosteroids';
  if (/gabapentin|pregabalin|levetiracetam|carbamazepin|lamotrigin|topiramat|valproat|phenytoin|levodopa|donepezil|memantine/.test(n)) return 'Neurology';
  if (/zolpidem|alprazolam|lorazepam|diazepam|clonazepam|midazolam|oxazepam|temazepam|zopiclone/.test(n)) return 'Sleep & Sedation';
  if (/sertralin|fluoxetin|paroxetin|escitalopram|citalopram|venlafaxin|bupropion|mirtazapin|amitriptylin|quetiapine|aripiprazol|olanzapin|risperidon|lithium/.test(n)) return 'Antidepressants';
  if (/vitamin|multivitamin|folic|foliumzuur|ijzer|ferr|calcium|zink|magnesium|cholecalciferol/.test(n)) return 'Vitamins & Supplements';
  if (/estradiol|levonorgestrel|ethinyl|norethisteron|progesteron|desogestrel|drospirenon/.test(n)) return "Women's Health";
  if (/tamsulosin|finasterid|sildenafil|tadalafil|oxybutynin|tolterodine/.test(n)) return 'Urology';
  if (/tamoxifen|anastrozol|imatinib|cyclophosphamide|methotrexaat.*kanker/.test(n)) return 'Oncology';
  if (/allopurinol|colchicin|cyclobenzaprin|baclofen|alendronaat|methocarbamol/.test(n)) return 'Joints & Muscles';
  if (/tretinoin|clobetasol|betamethason.*crème|tacrolimus.*crème|calcipotriol|mupirocin|minoxidil/.test(n)) return 'Skin & Wounds';
  if (/latanoprost|timolol.*oog|dorzolamide|brimonidine|ciproflox.*oog|tobramycin.*oog|kunsttranen/.test(n)) return 'Eye & Ear';
  if (/lidocaine|benzocaine|chlorhexidine|povidon.*jood|bacitracin|mupirocin.*wond/.test(n)) return 'First Aid';
  return null;
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
  const catKey    = findKey(/^category$/);  // directe categorie-kolom (bv. US, IT)
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

    // Gebruik directe category-kolom als ATC leeg is (bv. US openFDA data)
    const directCat = catKey ? String(row[catKey] || '').trim() : '';
    // Fallback: probeer naam/INN keyword matching wanneer ATC en Category beide leeg zijn
    const nameForCat = (inn || name || '').toLowerCase();
    const keywordCat = !atcToCategory(atc) && !directCat ? guessCategory(nameForCat) : null;
    const category = atcToCategory(atc) || directCat || keywordCat || null;
    if (!category) { skippedAtc++; continue; }

    const form = mapForm(formRaw);
    // Rx: UA/URA = Belgisch/Nederlands; "Rx" = directe waarde (US/CA/FR/IT scripts)
    const rx   = /\bUA\b|\bURA\b|recept|prescri/i.test(rxRaw) || rxRaw.trim() === 'Rx';

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
  console.log('\n🇬🇧 Verenigd Koninkrijk — NHSBSA BNF Code Information ophalen...');
  const country = loadExistingNames('gb');
  if (!country) { console.error('  ❌ gb.js niet gevonden'); return 0; }

  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_gb_medicines.py');

  if (!fs.existsSync(script)) {
    console.error('  ❌ fetch_gb_medicines.py niet gevonden');
    return 0;
  }

  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }

  console.log('  🐍 Python fetch script uitvoeren (NHSBSA CKAN API)...');
  try {
    execSync(`${python} "${script}"`, {
      stdio: 'inherit',
      timeout: 360_000,
      cwd: __dirname,
    });
  } catch (e) {
    console.error(`  ❌ Python script mislukt: ${e.message}`);
    return 0;
  }

  const dest = path.join(TMP_DIR, 'gb_medicines.csv');
  if (!fs.existsSync(dest)) {
    console.error('  ❌ gb_medicines.csv niet aangemaakt door script');
    return 0;
  }

  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size} bytes`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);

  return parseFile(dest, 'gb', country);
}

// ================================================================
// VERENIGDE STATEN — openFDA Human Drug NDC Directory
// Dagelijks bijgewerkte JSON-dump van alle FDA-geregistreerde middelen.
// Manifest: https://api.fda.gov/download.json → drug.ndc.partitions[].file
// ================================================================
async function updateUS() {
  console.log('\n🇺🇸 Verenigde Staten — openFDA NDC Directory ophalen...');
  const country = loadExistingNames('us');
  if (!country) { console.error('  ❌ us.js niet gevonden'); return 0; }

  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_us_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_us_medicines.py niet gevonden'); return 0; }

  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }

  console.log('  🐍 Python fetch script uitvoeren (openFDA NDC)...');
  try {
    execSync(`${python} "${script}"`, { stdio: 'inherit', timeout: 600_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }

  const dest = path.join(TMP_DIR, 'us_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ us_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'us', country);
}

// ================================================================
// CANADA — Health Canada Drug Product Database (DPD)
// Nachtelijks bijgewerkte ZIP met goedgekeurde Canadese medicijnen.
// URL: https://open.canada.ca/data/... (open.canada.ca open data)
// Bevat ATC-codes, merknamen, INN en farmaceutische vormen.
// ================================================================
async function updateCA() {
  console.log('\n🇨🇦 Canada — Health Canada DPD ophalen...');
  const country = loadExistingNames('ca');
  if (!country) { console.error('  ❌ ca.js niet gevonden'); return 0; }

  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_ca_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_ca_medicines.py niet gevonden'); return 0; }

  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }

  console.log('  🐍 Python fetch script uitvoeren (Health Canada DPD)...');
  try {
    execSync(`${python} "${script}"`, { stdio: 'inherit', timeout: 360_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }

  const dest = path.join(TMP_DIR, 'ca_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ ca_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'ca', country);
}

// ================================================================
// FRANKRIJK — ANSM Base de Données Publique des Médicaments (BDPM)
// Maandelijks bijgewerkt, directe download zonder authenticatie.
// Bestanden: CIS_bdpm.txt (merknamen) + CIS_COMPO_bdpm.txt (INN)
// ================================================================
async function updateFR() {
  console.log('\n🇫🇷 Frankrijk — ANSM BDPM ophalen...');
  const country = loadExistingNames('fr');
  if (!country) { console.error('  ❌ fr.js niet gevonden'); return 0; }

  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_fr_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_fr_medicines.py niet gevonden'); return 0; }

  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }

  console.log('  🐍 Python fetch script uitvoeren...');
  try {
    execSync(`${python} "${script}"`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }

  const dest = path.join(TMP_DIR, 'fr_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ fr_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'fr', country);
}

// ================================================================
// ITALIË — AIFA Transparency List + AIC Register
// AIFA publiceert maandelijks een CSV van alle vergunde medicijnen
// met ATC-codes, merknamen en prijsklassen.
// ================================================================
async function updateIT() {
  console.log('\n🇮🇹 Italië — AIFA Transparency List ophalen...');
  const country = loadExistingNames('it');
  if (!country) { console.error('  ❌ it.js niet gevonden'); return 0; }

  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_it_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_it_medicines.py niet gevonden'); return 0; }

  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }

  console.log('  🐍 Python fetch script uitvoeren...');
  try {
    execSync(`${python} "${script}"`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }

  const dest = path.join(TMP_DIR, 'it_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ it_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'it', country);
}


async function updateAT() {
  console.log('\n🇦🇹 Oostenrijk — EMA + nationale bron ophalen...');
  const country = loadExistingNames('at');
  if (!country) { console.error('  ❌ at.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" at`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'at_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ at_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'at', country);
}

async function updateCH() {
  console.log('\n🇨🇭 Zwitserland — EMA + nationale bron ophalen...');
  const country = loadExistingNames('ch');
  if (!country) { console.error('  ❌ ch.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" ch`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'ch_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ ch_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'ch', country);
}

async function updateDK() {
  console.log('\n🇩🇰 Denemarken — EMA + nationale bron ophalen...');
  const country = loadExistingNames('dk');
  if (!country) { console.error('  ❌ dk.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" dk`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'dk_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ dk_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'dk', country);
}

async function updateES() {
  console.log('\n🇪🇸 Spanje — EMA + nationale bron ophalen...');
  const country = loadExistingNames('es');
  if (!country) { console.error('  ❌ es.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" es`, { stdio: 'inherit', timeout: 600_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'es_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ es_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'es', country);
}

async function updateFI() {
  console.log('\n🇫🇮 Finland — EMA + nationale bron ophalen...');
  const country = loadExistingNames('fi');
  if (!country) { console.error('  ❌ fi.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" fi`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'fi_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ fi_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'fi', country);
}

async function updateIE() {
  console.log('\n🇮🇪 Ierland — EMA + nationale bron ophalen...');
  const country = loadExistingNames('ie');
  if (!country) { console.error('  ❌ ie.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" ie`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'ie_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ ie_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'ie', country);
}

async function updateNO() {
  console.log('\n🇳🇴 Noorwegen — EMA + nationale bron ophalen...');
  const country = loadExistingNames('no');
  if (!country) { console.error('  ❌ no.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" no`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'no_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ no_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'no', country);
}

async function updatePL() {
  console.log('\n🇵🇱 Polen — EMA + nationale bron ophalen...');
  const country = loadExistingNames('pl');
  if (!country) { console.error('  ❌ pl.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" pl`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'pl_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ pl_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'pl', country);
}

async function updatePT() {
  console.log('\n🇵🇹 Portugal — EMA + nationale bron ophalen...');
  const country = loadExistingNames('pt');
  if (!country) { console.error('  ❌ pt.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" pt`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'pt_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ pt_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'pt', country);
}

async function updateSE() {
  console.log('\n🇸🇪 Zweden — EMA + nationale bron ophalen...');
  const country = loadExistingNames('se');
  if (!country) { console.error('  ❌ se.js niet gevonden'); return 0; }
  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_eu_medicines.py niet gevonden'); return 0; }
  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }
  try {
    execSync(`${python} "${script}" se`, { stdio: 'inherit', timeout: 300_000, cwd: __dirname });
  } catch (e) {
    console.error(`  ❌ Script mislukt: ${e.message}`); return 0;
  }
  const dest = path.join(TMP_DIR, 'se_medicines.csv');
  if (!fs.existsSync(dest)) { console.error('  ❌ se_medicines.csv niet aangemaakt'); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 1000) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, 'se', country);
}

// ================================================================
// NIEUWE EU-LANDEN (CZ, SK, HR, SI, HU, RO, GR, LU)
// Gebruikt fetch_new_eu_medicines.py <landcode>
// ================================================================
async function updateNewEuCountry(code, label) {
  console.log(`\n${label} — fetch_new_eu_medicines.py ${code.toUpperCase()} ophalen...`);
  const country = loadExistingNames(code);
  if (!country) { console.error(`  ❌ ${code}.js niet gevonden`); return 0; }

  const { execSync } = require('child_process');
  const script = path.join(__dirname, 'fetch_new_eu_medicines.py');
  if (!fs.existsSync(script)) { console.error('  ❌ fetch_new_eu_medicines.py niet gevonden'); return 0; }

  let python = 'python3';
  try { execSync('python3 --version', { stdio: 'ignore' }); }
  catch { try { execSync('python --version', { stdio: 'ignore' }); python = 'python'; } catch { console.error('  ❌ Python niet gevonden'); return 0; } }

  try { execSync(`${python} -c "import openpyxl"`, { stdio: 'ignore' }); }
  catch {
    console.log('  📦 openpyxl installeren...');
    try { execSync(`${python} -m pip install openpyxl --quiet`, { stdio: 'pipe' }); } catch {}
  }

  console.log(`  🐍 fetch_new_eu_medicines.py ${code.toUpperCase()} uitvoeren...`);
  try {
    execSync(`${python} "${script}" ${code.toUpperCase()}`, {
      stdio: 'inherit', timeout: 300_000, cwd: __dirname,
    });
  } catch (e) { console.error(`  ❌ Script mislukt: ${e.message}`); return 0; }

  const dest = path.join(TMP_DIR, `${code}_medicines.csv`);
  if (!fs.existsSync(dest)) { console.error(`  ❌ ${code}_medicines.csv niet aangemaakt`); return 0; }
  const size = fs.statSync(dest).size;
  if (size < 500) { console.error(`  ❌ CSV te klein: ${size}B`); return 0; }
  console.log(`  ✅ CSV gereed: ${(size/1024).toFixed(0)} KB`);
  return parseFile(dest, code, country);
}

async function updateCZ() { return updateNewEuCountry('cz', '🇨🇿 Tsjechië'); }
async function updateSK() { return updateNewEuCountry('sk', '🇸🇰 Slowakije'); }
async function updateHR() { return updateNewEuCountry('hr', '🇭🇷 Kroatië'); }
async function updateSI() { return updateNewEuCountry('si', '🇸🇮 Slovenië'); }
async function updateHU() { return updateNewEuCountry('hu', '🇭🇺 Hongarije'); }
async function updateRO() { return updateNewEuCountry('ro', '🇷🇴 Roemenië'); }
async function updateGR() { return updateNewEuCountry('gr', '🇬🇷 Griekenland'); }
async function updateLU() { return updateNewEuCountry('lu', '🇱🇺 Luxemburg'); }

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
    else if (target === 'us') added = await updateUS();
    else if (target === 'ca') added = await updateCA();
    else if (target === 'fr') added = await updateFR();
    else if (target === 'it') added = await updateIT();
    else if (target === 'at') added = await updateAT();
    else if (target === 'ch') added = await updateCH();
    else if (target === 'dk') added = await updateDK();
    else if (target === 'es') added = await updateES();
    else if (target === 'fi') added = await updateFI();
    else if (target === 'ie') added = await updateIE();
    else if (target === 'no') added = await updateNO();
    else if (target === 'pl') added = await updatePL();
    else if (target === 'pt') added = await updatePT();
    else if (target === 'se') added = await updateSE();
    else if (target === 'cz') added = await updateCZ();
    else if (target === 'sk') added = await updateSK();
    else if (target === 'hr') added = await updateHR();
    else if (target === 'si') added = await updateSI();
    else if (target === 'hu') added = await updateHU();
    else if (target === 'ro') added = await updateRO();
    else if (target === 'gr') added = await updateGR();
    else if (target === 'lu') added = await updateLU();
    else { console.log(`⚠️  Onbekend land: ${target}`); continue; }

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
