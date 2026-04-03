#!/usr/bin/env python3
"""
🇩🇪 apoHouze — Duitsland Medicijnen Fetcher v4
================================================
Bron: EMA JSON rapport (europa.eu open data)
Alle centraal vergunde EU-geneesmiddelen — beschikbaar in alle EU-landen incl. DE
Twee keer per dag bijgewerkt door EMA
"""
import sys, os, csv, re, json, urllib.request

OUTPUT_FILE = os.environ.get('DE_OUTPUT', '/tmp/de_medicines.csv')

print("🇩🇪 apoHouze — Duitsland Medicijnen Fetcher v4")
print("=" * 52)
print("📌 Bron: EMA JSON rapport (europa.eu open data)")

EMA_JSON_URL = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json"

# ATC → category (eerste 3 tekens)
ATC_MAP = {
    'A02':'Stomach & Intestine','A03':'Stomach & Intestine','A04':'Stomach & Intestine',
    'A05':'Stomach & Intestine','A06':'Stomach & Intestine','A07':'Stomach & Intestine',
    'A08':'Stomach & Intestine','A09':'Stomach & Intestine','A10':'Diabetes',
    'A11':'Vitamins & Supplements','A12':'Vitamins & Supplements',
    'A13':'Vitamins & Supplements','A16':'Stomach & Intestine',
    'B01':'Anticoagulants','B02':'Heart & Blood Pressure','B03':'Vitamins & Supplements',
    'B05':'Heart & Blood Pressure','B06':'Heart & Blood Pressure',
    'C01':'Heart & Blood Pressure','C02':'Heart & Blood Pressure',
    'C03':'Heart & Blood Pressure','C04':'Heart & Blood Pressure',
    'C05':'Heart & Blood Pressure','C07':'Heart & Blood Pressure',
    'C08':'Heart & Blood Pressure','C09':'Heart & Blood Pressure','C10':'Cholesterol',
    'D01':'Antifungals','D02':'Skin & Wounds','D03':'Skin & Wounds',
    'D04':'Skin & Wounds','D05':'Skin & Wounds','D06':'Antibiotics',
    'D07':'Corticosteroids','D08':'Skin & Wounds','D09':'Skin & Wounds',
    'D10':'Skin & Wounds','D11':'Skin & Wounds',
    'G01':"Women's Health",'G02':"Women's Health",'G03':"Women's Health",
    'G04':'Urology',
    'H01':'Thyroid','H02':'Corticosteroids','H03':'Thyroid',
    'H04':'Diabetes','H05':'Vitamins & Supplements',
    'J01':'Antibiotics','J02':'Antifungals','J04':'Antibiotics',
    'J05':'Antivirals','J06':'Antivirals','J07':'Antivirals',
    'L01':'Oncology','L02':'Oncology','L03':'Oncology','L04':'Corticosteroids',
    'M01':'Pain & Fever','M02':'Joints & Muscles','M03':'Joints & Muscles',
    'M04':'Joints & Muscles','M05':'Joints & Muscles','M09':'Joints & Muscles',
    'N01':'Pain & Fever','N02':'Pain & Fever','N03':'Neurology',
    'N04':'Neurology','N05':'Sleep & Sedation','N06':'Antidepressants',
    'N07':'Nervous System',
    'P01':'Antiparasitics','P02':'Antiparasitics','P03':'Antiparasitics',
    'R01':'Cough & Cold','R02':'Cough & Cold','R03':'Lungs & Asthma',
    'R04':'Cough & Cold','R05':'Cough & Cold','R06':'Allergy','R07':'Lungs & Asthma',
    'S01':'Eye & Ear','S02':'Eye & Ear','S03':'Eye & Ear',
    'V03':'First Aid','V06':'Vitamins & Supplements','V07':'First Aid','V08':'First Aid',
}

def atc_to_category(atc):
    return ATC_MAP.get((atc or '').strip()[:3].upper())

def map_form(text):
    t = (text or '').lower()
    if re.search(r'effervesc',t):         return 'Effervescent tablet'
    if re.search(r'orodispers|dispersi',t): return 'Dispersible tablet'
    if re.search(r'eye drop|ophthalm',t): return 'Eye drops'
    if re.search(r'ear drop|otic',t):     return 'Ear drops'
    if re.search(r'nasal spray',t):       return 'Nasal spray'
    if re.search(r'inhaler|inhal|aerosol',t): return 'Inhaler'
    if re.search(r'tablet|film-coat',t):  return 'Tablet'
    if re.search(r'capsule|hard cap',t):  return 'Capsule'
    if re.search(r'syrup|oral sol',t):    return 'Syrup'
    if re.search(r'\bdrops\b',t):         return 'Drops'
    if re.search(r'\bcream\b',t):         return 'Cream'
    if re.search(r'ointment',t):          return 'Ointment'
    if re.search(r'\bgel\b',t):           return 'Gel'
    if re.search(r'patch|transdermal',t): return 'Patch'
    if re.search(r'\bspray\b',t):         return 'Spray'
    if re.search(r'inject|infusion|i\.v\.',t): return 'Injection'
    if re.search(r'suppository|suppos',t): return 'Suppository'
    if re.search(r'powder',t):            return 'Powder'
    if re.search(r'suspension',t):        return 'Suspension'
    if re.search(r'solution',t):          return 'Solution'
    return 'Tablet'

# ------------------------------------------------------------------
print("\n[1/3] EMA JSON ophalen...")
print(f"  📥 {EMA_JSON_URL}")

try:
    req = urllib.request.Request(EMA_JSON_URL,
        headers={'User-Agent': 'Mozilla/5.0 apoHouze-updater/4.0'})
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read()
    print(f"  ✅ {len(raw)//1024} KB gedownload")
    data = json.loads(raw)
except Exception as e:
    print(f"  ❌ Download mislukt: {e}")
    sys.exit(1)

# EMA JSON structure: {"medicines": [...]} or list directly
if isinstance(data, dict):
    records = data.get('medicines', data.get('results', data.get('data', [])))
    # Try to find the array
    if not records:
        for v in data.values():
            if isinstance(v, list) and len(v) > 10:
                records = v
                break
elif isinstance(data, list):
    records = data
else:
    records = []

print(f"  📊 {len(records)} records geladen")

if not records:
    print("  ❌ Geen records gevonden in JSON")
    sys.exit(1)

# Show sample keys
sample = records[0] if records else {}
if isinstance(sample, dict):
    print(f"  🔑 Sleutels: {', '.join(list(sample.keys())[:10])}")

# ------------------------------------------------------------------
print("\n[2/3] Verwerken...")

# Find field names dynamically
def find_field(record, *patterns):
    for k in record.keys():
        kl = k.lower().replace('_','').replace('-','').replace(' ','')
        for p in patterns:
            if p in kl:
                return k
    return None

if records and isinstance(records[0], dict):
    r0 = records[0]
    name_key  = find_field(r0, 'medicinename','productname','brandname','name','title')
    inn_key   = find_field(r0, 'activesubstance','inn','substance','ingredient','generic')
    atc_key   = find_field(r0, 'atccode','atc')
    form_key  = find_field(r0, 'pharmaceuticalform','form','dosageform')
    status_key= find_field(r0, 'authorisationstatus','status','marketingstatus')
    print(f"  📋 name:{name_key} | inn:{inn_key} | atc:{atc_key} | form:{form_key} | status:{status_key}")
else:
    print("  ❌ Records zijn geen dict-objecten")
    sys.exit(1)

if not name_key:
    print(f"  ❌ Naamveld niet gevonden. Beschikbare velden: {', '.join(list(r0.keys())[:15])}")
    sys.exit(1)

medicines = {}
valid = revoked = no_atc = blacklisted = 0

BLACKLIST = re.compile(
    r'\b(device|test kit|diagnostic|reagent|radiopharm|contrast medium|'
    r'parenteral nutrition|dialysis|disinfectant)\b', re.IGNORECASE)

for rec in records:
    if not isinstance(rec, dict):
        continue
    
    name = str(rec.get(name_key, '') or '').strip()
    if not name or len(name) < 2:
        continue
    
    # Status filter: skip withdrawn/refused/expired
    status = str(rec.get(status_key, '') or '').lower()
    if status and re.search(r'withdrawn|refused|expired|revoked|cancelled', status):
        revoked += 1
        continue
    
    atc   = str(rec.get(atc_key, '') or '').strip() if atc_key else ''
    inn   = str(rec.get(inn_key, '') or '').strip() if inn_key else ''
    form  = str(rec.get(form_key, '') or '').strip() if form_key else ''
    
    # Blacklist check
    if BLACKLIST.search(name) or (inn and BLACKLIST.search(inn)):
        blacklisted += 1
        continue
    
    category = atc_to_category(atc)
    if not category:
        no_atc += 1
        continue
    
    key = name.lower()
    if key not in medicines:
        medicines[key] = {
            'Name': name,
            'INN': inn,
            'ATC': atc,
            'PharmaceuticalForm': map_form(form),
            'RxStatus': 'UA',
            'Country': 'DE',
            'Category': category,
        }
        valid += 1

print(f"  ✅ {valid} geldig | {revoked} ingetrokken | {no_atc} geen ATC | {blacklisted} blacklist")
print(f"  🎯 Na dedup: {len(medicines)} unieke medicijnen")

if not medicines:
    print("  ❌ Geen geldige medicijnen na filtering")
    sys.exit(1)

# ------------------------------------------------------------------
print("\n[3/3] Opslaan...")
os.makedirs(os.path.dirname(OUTPUT_FILE) or '.', exist_ok=True)
fieldnames = ['Name','INN','ATC','PharmaceuticalForm','RxStatus','Country','Category']
with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(medicines.values())

size = os.path.getsize(OUTPUT_FILE) // 1024
print(f"✅ {len(medicines)} medicijnen opgeslagen → {OUTPUT_FILE} ({size} KB)")
