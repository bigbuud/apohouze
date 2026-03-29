#!/usr/bin/env node
/**
 * apoHouze — Medicine Database Updater v4
 * ========================================
 * BE: SAM v2 via directe bekende URL-patronen
 * NL: CBG Geneesmiddeleninformatiebank
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
// ATC-CODE MAPPING (eerste 3 chars)
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
  // ATC kan zijn: "A02BC01", "A02BC", "A02" — neem altijd de eerste 3 chars
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
  [/inhalator|inhaler|aerosol/i,          'Inhaler'],
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
function fetchBinary(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    const go = (u) => {
      const proto = u.startsWith('https') ? https : http;
      proto.get(u, {
        headers: {
          'User-Agent': 'Mozilla/5.0 apoHouze-updater/4.0',
          'Accept': '*/*',
        }
      }, res => {
        if ([301, 302, 303, 307, 308].includes(res.statusCode)) {
          file.close();
          const loc = res.headers.location;
          const nextUrl = loc.startsWith('http') ? loc : new URL(loc, u).href;
          return go(nextUrl);
        }
        if (res.statusCode !== 200) {
          file.close();
          return reject(new Error(`HTTP ${res.statusCode} — ${u}`));
        }
        res.pipe(file);
        file.on('finish', () => { file.close(); resolve(); });
        file.on('error', reject);
      }).on('error', reject);
    };
    go(url);
  });
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    const go = (u) => {
      const proto = u.startsWith('https') ? https : http;
      proto.get(u, { headers: { 'User-Agent': 'apoHouze-updater/4.0' } }, res => {
        if ([301, 302, 303, 307, 308].includes(res.statusCode)) {
          const loc = res.headers.location;
          return go(loc.startsWith('http') ? loc : new URL(loc, u).href);
        }
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve(data));
      }).on('error', reject);
    };
    go(url);
  });
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
  const insertAt = content.lastIndexOf('\n];');
  if (insertAt === -1) return 0;
  const lines = medicines.map(m =>
    `  { name: ${JSON.stringify(m.name)}, generic: ${JSON.stringify(m.generic||'')}, ` +
    `category: ${JSON.stringify(m.category)}, form: ${JSON.stringify(m.form)}, rx: ${m.rx} },`
  ).join('\n');
  const updated = content.slice(0, insertAt) + '\n' + lines + '\n' + content.slice(insertAt);
  if (!DRY_RUN) fs.writeFileSync(fp, updated, 'utf8');
  return medicines.length;
}

// ================================================================
// BELGIË — SAM v2
// De SAM-pagina laadt links via JavaScript. We proberen bekende
// URL-patronen rechtstreeks. De export-URL bevat een timestamp
// in milliseconden die we via de REST-endpoint ophalen.
// ================================================================
async function updateBE() {
  console.log('\n🇧🇪 België — SAM v2 ophalen...');
  const country = loadExistingNames('be');
  if (!country) { console.error('  ❌ be.js niet gevonden'); return 0; }

  // Probeer de export-lijst op te halen via het JSON-endpoint
  // dat de JavaScript-pagina gebruikt
  const apiUrls = [
    'https://www.vas.ehealth.fgov.be/websamcivics/samcivics/rest/export/v4/latestId',
    'https://www.vas.ehealth.fgov.be/websamcivics/samcivics/api/export/latest?version=4',
    'https://www.vas.ehealth.fgov.be/websamcivics/samcivics/export/latest/4',
  ];

  let exportId = null;
  for (const url of apiUrls) {
    try {
      const text = await fetchText(url);
      const match = text.match(/\d{10,}/);
      if (match) { exportId = match[0]; break; }
    } catch {}
  }

  // Probeer ZIP direct te downloaden met bekende patronen
  const zipUrls = [];
  if (exportId) {
    zipUrls.push(
      `https://www.vas.ehealth.fgov.be/websamcivics/samcivics/export/v4/${exportId}`,
      `https://www.vas.ehealth.fgov.be/websamcivics/samcivics/download/v4/${exportId}`,
    );
  }
  // Vaste URL-patronen als fallback
  zipUrls.push(
    'https://www.vas.ehealth.fgov.be/websamcivics/samcivics/rest/v4/export/full',
    'https://www.vas.ehealth.fgov.be/websamcivics/samcivics/export?version=4&type=full',
    'https://www.vas.ehealth.fgov.be/websamcivics/samcivics/v4/full.zip',
    'https://www.vas.ehealth.fgov.be/websamcivics/samcivics/SAMv2Full_v4.zip',
  );

  const zipDest = path.join(TMP_DIR, 'sam_be.zip');
  let downloaded = false;

  for (const url of zipUrls) {
    try {
      await fetchBinary(url, zipDest);
      const size = fs.statSync(zipDest).size;
      if (size > 100000) { // minstens 100KB = echte ZIP
        console.log(`  ✅ SAM ZIP gedownload van: ${url} (${(size/1024/1024).toFixed(1)} MB)`);
        downloaded = true;
        break;
      }
    } catch {}
    if (fs.existsSync(zipDest)) fs.unlinkSync(zipDest);
  }

  if (!downloaded) {
    console.error('  ❌ SAM ZIP kon niet automatisch gedownload worden.');
    console.log('  💡 Ga naar https://www.vas.ehealth.fgov.be/websamcivics/samcivics/');
    console.log('     Download de ZIP manueel en kopieer het bestand naar data/_tmp/sam_be.zip');
    console.log('     Voer dan opnieuw uit: node update.js be');
    // Controleer of er een manueel geplaatst bestand is
    const manual = path.join(DATA_DIR, '..', 'sam_be.zip');
    if (fs.existsSync(manual)) {
      console.log('  📁 Manueel bestand gevonden! Verwerken...');
      fs.copyFileSync(manual, zipDest);
    } else {
      return 0;
    }
  }

  return parseSAMZip(zipDest, country);
}

function parseSAMZip(zipPath, country) {
  const { execSync } = require('child_process');
  const extractDir = path.join(TMP_DIR, 'sam_extract');
  if (fs.existsSync(extractDir)) execSync(`rm -rf "${extractDir}"`);
  fs.mkdirSync(extractDir);

  try {
    execSync(`unzip -o "${zipPath}" -d "${extractDir}" 2>/dev/null`);
  } catch (e) {
    console.error(`  ❌ ZIP extraheren mislukt: ${e.message}`);
    return 0;
  }

  // Zoek AMP XML-bestand (bevat de Actual Medicinal Products)
  const xmlFiles = [];
  function findXml(dir) {
    for (const f of fs.readdirSync(dir)) {
      const full = path.join(dir, f);
      if (fs.statSync(full).isDirectory()) findXml(full);
      else if (f.endsWith('.xml')) xmlFiles.push(full);
    }
  }
  findXml(extractDir);

  if (!xmlFiles.length) {
    console.error('  ❌ Geen XML-bestanden gevonden in ZIP');
    console.log('  📁 Inhoud ZIP:', fs.readdirSync(extractDir).join(', '));
    return 0;
  }

  // Kies het AMP-bestand (grootste XML of met "AMP" in naam)
  const ampFile = xmlFiles.find(f => /AMP/i.test(path.basename(f)))
               || xmlFiles.sort((a,b) => fs.statSync(b).size - fs.statSync(a).size)[0];

  console.log(`  📄 Parsen: ${path.basename(ampFile)} (${(fs.statSync(ampFile).size/1024/1024).toFixed(1)} MB)`);

  const xml = fs.readFileSync(ampFile, 'utf8');
  const newMeds = [];
  const seen = new Set(country.names);

  // SAM AMP XML-elementen: <Amp>, <Name>, <Atc>, <AdministrationForm>
  // Naam staat in <Name lang="NL"> of <Name lang="FR">
  const ampRe = /<Amp\b[\s\S]*?<\/Amp>/gi;
  let match;
  while ((match = ampRe.exec(xml)) !== null) {
    const block = match[0];

    // Naam: voorkeur NL, anders FR
    const nlName = (block.match(/lang="NL"[^>]*>([^<]+)</) || [])[1]?.trim();
    const frName = (block.match(/lang="FR"[^>]*>([^<]+)</) || [])[1]?.trim();
    const name = nlName || frName;
    if (!name || seen.has(name.toLowerCase())) continue;

    // ATC-code
    const atc = (block.match(/<Atc>([^<]+)<\/Atc>/) || [])[1]?.trim() || '';
    const category = atcToCategory(atc);
    if (!category) continue;

    // Werkzame stof
    const inn = (block.match(/<Inn>([^<]+)<\/Inn>/) ||
                 block.match(/<ActiveIngredient>([^<]+)<\/ActiveIngredient>/) || [])[1]?.trim() || '';

    // Farmaceutische vorm
    const formRaw = (block.match(/<AdministrationFormName[^>]*>([^<]+)</) ||
                     block.match(/<PharmaceuticalForm[^>]*>([^<]+)</) || [])[1]?.trim() || '';
    const form = mapForm(formRaw);

    // Receptplichtig
    const rxRaw = (block.match(/<PrescriptionRequired>([^<]+)<\/PrescriptionRequired>/) || [])[1] || '';
    const rx = /true|yes|1/i.test(rxRaw);

    newMeds.push({ name, generic: inn, category, form, rx });
    seen.add(name.toLowerCase());
  }

  console.log(`  📊 ${newMeds.length} nieuwe medicijnen gevonden`);
  return appendMedicines('be', newMeds);
}

// ================================================================
// NEDERLAND — CBG Geneesmiddeleninformatiebank
// Kolommen: registratienummer, productnaam, productnaam_link,
//            atc, werkzame_stof, farmaceutische_vorm, ...
// ================================================================
async function updateNL() {
  console.log('\n🇳🇱 Nederland — CBG ophalen...');
  const country = loadExistingNames('nl');
  if (!country) { console.error('  ❌ nl.js niet gevonden'); return 0; }

  // Bekende download-URLs voor het CBG-databestand
  const urls = [
    // Open data via data.overheid.nl
    'https://data.overheid.nl/community/application/geneesmiddelenrepertorium-cbg/download/databestand',
    // Directe databestand download van CBG
    'https://geneesmiddelenrepertorium.nl/ords/f?p=111:download:0::NO',
    // Fallback: open state
    'https://data.openstate.eu/dataset/2e0055db-6f28-4b05-920b-a648ba026baa/resource/1efaa651-add9-40f5-8b0c-2c2f2d352e11/download/geneesmiddeleninformatiebank.csv',
  ];

  const dest = path.join(TMP_DIR, 'cbg_nl.csv');
  let downloaded = false;

  for (const url of urls) {
    try {
      await fetchBinary(url, dest);
      const size = fs.statSync(dest).size;
      if (size > 50000) {
        console.log(`  ✅ CBG databestand gedownload (${(size/1024).toFixed(0)} KB) van: ${url}`);
        downloaded = true;
        break;
      }
    } catch (e) {
      console.log(`  ⚠️  ${url}: ${e.message}`);
    }
    if (fs.existsSync(dest)) fs.unlinkSync(dest);
  }

  if (!downloaded) {
    console.error('  ❌ CBG databestand kon niet gedownload worden');
    return 0;
  }

  return parseCBGData(dest, country);
}

function parseCBGData(filePath, country) {
  // Lees bestand met UTF-8, strip BOM
  let content = fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, '');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length < 2) return 0;

  // Detecteer separator: tab, puntkomma of komma
  const firstLine = lines[0];
  const sep = firstLine.includes('\t') ? '\t'
            : firstLine.includes(';')  ? ';'
            : ',';

  const rawHeaders = firstLine.split(sep).map(h => h.replace(/^"|"$/g, '').trim().toLowerCase());
  console.log(`  📋 Kolommen (${rawHeaders.length}): ${rawHeaders.slice(0,8).join(', ')}...`);
  console.log(`  📋 Separator: ${sep === '\t' ? 'TAB' : sep}`);

  // Zoek kolom-indexen op basis van bekende CBG-kolomnamen
  const find = (...patterns) => rawHeaders.findIndex(h => patterns.some(p => p.test(h)));

  const nameIdx = find(/^productnaam$/, /^naam$/, /^product_name$/, /^name$/);
  const innIdx  = find(/^werkzame_stof$/, /^inn$/, /^actieve_stof$/, /^substance$/);
  const atcIdx  = find(/^atc$/, /^atc_code$/, /^atccode$/);
  const formIdx = find(/^farmaceutische_vorm$/, /^vorm$/, /^pharmaceutical_form$/, /^toedieningsvorm$/);
  const rxIdx   = find(/^afleverstatus$/, /^recept$/, /^rx$/, /^prescri/, /^ura$/);

  console.log(`  📋 name:${nameIdx} inn:${innIdx} atc:${atcIdx} form:${formIdx} rx:${rxIdx}`);

  if (nameIdx === -1) {
    console.error('  ❌ Naamkolom niet gevonden. Beschikbare kolommen:');
    console.error('    ', rawHeaders.join(', '));
    return 0;
  }

  const newMeds = [];
  const seen = new Set(country.names);
  let skippedNoAtc = 0, skippedExists = 0;

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols = line.split(sep).map(c => c.replace(/^"|"$/g, '').trim());

    const name = cols[nameIdx];
    if (!name) continue;
    if (seen.has(name.toLowerCase())) { skippedExists++; continue; }

    const inn     = innIdx  >= 0 ? cols[innIdx]  || '' : '';
    const atcCode = atcIdx  >= 0 ? cols[atcIdx]  || '' : '';
    const formRaw = formIdx >= 0 ? cols[formIdx] || '' : '';
    const rxRaw   = rxIdx   >= 0 ? cols[rxIdx]   || '' : '';

    const category = atcToCategory(atcCode);
    if (!category) { skippedNoAtc++; continue; }

    const form = mapForm(formRaw);
    // UA = uitsluitend op recept (Netherlands)
    const rx = /\bUA\b|\bURA\b|recept|prescri/i.test(rxRaw);

    newMeds.push({ name, generic: inn, category, form, rx });
    seen.add(name.toLowerCase());
  }

  console.log(`  📊 Gevonden: ${newMeds.length} nieuw | Bestond al: ${skippedExists} | Geen ATC: ${skippedNoAtc}`);
  return appendMedicines('nl', newMeds);
}

// ================================================================
// HOOFD
// ================================================================
async function main() {
  console.log('\n🔄 apoHouze Medicine Database Updater v4');
  console.log(`📅 ${new Date().toISOString()}`);
  if (DRY_RUN) console.log('🔍 DRY RUN — geen bestanden worden gewijzigd');

  const log = { updated_at: new Date().toISOString(), dry_run: DRY_RUN, results: {} };
  let totalAdded = 0;

  for (const target of targets) {
    const before = loadExistingNames(target)?.names.size || 0;
    let added = 0;

    if (target === 'be') added = await updateBE();
    else if (target === 'nl') added = await updateNL();
    else { console.log(`⚠️  Onbekend land: ${target}`); continue; }

    const after = loadExistingNames(target)?.names.size || 0;
    log.results[target] = { before, after, added: after - before };
    totalAdded += (after - before);
    console.log(`\n  ✅ ${target.toUpperCase()}: ${before} → ${after} medicijnen (+${after - before} nieuw)\n`);
  }

  // Opruimen
  try { require('child_process').execSync(`rm -rf "${TMP_DIR}"`); } catch {}

  if (!DRY_RUN) fs.writeFileSync(LOG_FILE, JSON.stringify(log, null, 2));

  console.log(`🎉 Klaar! Totaal toegevoegd: ${totalAdded} nieuwe medicijnen`);
  if (totalAdded > 0 && !DRY_RUN) console.log('🚀 Commit en push om Docker rebuild te triggeren.');
  process.exit(0);
}

main().catch(err => { console.error('❌ Fout:', err.message); process.exit(1); });
