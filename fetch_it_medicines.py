#!/usr/bin/env python3
"""
apoHouze — Italië Medicijnen Fetcher v2
=========================================
Bron 1: EMA JSON (dezelfde als DE maar met Italiaanse INN-naam matching)
  Alle EMA-vergunde middelen zijn ook in Italië beschikbaar.
  URL: https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json

Bron 2: AIFA elenco medicinali carenti (shortage list met ATC-codes)
  Bevat Italiaanse merknamen + ATC + INN - URL is wél bereikbaar vanuit CI.
  URL: https://www.aifa.gov.it/documents/20142/847339/elenco_medicinali_carenti.csv

Categorisatie via ATC-code (ATC_MAP identiek aan update.js).
"""

import sys, os, re, csv, time, subprocess, json, io

DEBUG = "--debug" in sys.argv
REPO_ROOT   = os.getcwd()
TMP_DIR     = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "it_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

EMA_JSON_URLS = [
    "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json",
]

AIFA_CARENTI_URL = "https://www.aifa.gov.it/documents/20142/847339/elenco_medicinali_carenti.csv"

ATC_MAP = {
    "A02":"Stomach & Intestine","A03":"Stomach & Intestine","A04":"Stomach & Intestine",
    "A05":"Stomach & Intestine","A06":"Stomach & Intestine","A07":"Stomach & Intestine",
    "A08":"Stomach & Intestine","A09":"Stomach & Intestine","A10":"Diabetes",
    "A11":"Vitamins & Supplements","A12":"Vitamins & Supplements","A13":"Vitamins & Supplements",
    "A16":"Stomach & Intestine",
    "B01":"Anticoagulants","B02":"Heart & Blood Pressure","B03":"Vitamins & Supplements",
    "B05":"Heart & Blood Pressure","B06":"Heart & Blood Pressure",
    "C01":"Heart & Blood Pressure","C02":"Heart & Blood Pressure","C03":"Heart & Blood Pressure",
    "C04":"Heart & Blood Pressure","C05":"Heart & Blood Pressure","C07":"Heart & Blood Pressure",
    "C08":"Heart & Blood Pressure","C09":"Heart & Blood Pressure","C10":"Cholesterol",
    "D01":"Antifungals","D02":"Skin & Wounds","D03":"Skin & Wounds","D04":"Skin & Wounds",
    "D05":"Skin & Wounds","D06":"Antibiotics","D07":"Corticosteroids","D08":"Skin & Wounds",
    "D09":"Skin & Wounds","D10":"Skin & Wounds","D11":"Skin & Wounds",
    "G01":"Women's Health","G02":"Women's Health","G03":"Women's Health","G04":"Urology",
    "H01":"Thyroid","H02":"Corticosteroids","H03":"Thyroid","H04":"Diabetes",
    "H05":"Vitamins & Supplements",
    "J01":"Antibiotics","J02":"Antifungals","J04":"Antibiotics","J05":"Antivirals",
    "J06":"Antivirals","J07":"Antivirals",
    "L01":"Oncology","L02":"Oncology","L03":"Oncology","L04":"Corticosteroids",
    "M01":"Pain & Fever","M02":"Joints & Muscles","M03":"Joints & Muscles",
    "M04":"Joints & Muscles","M05":"Joints & Muscles","M09":"Joints & Muscles",
    "N01":"Pain & Fever","N02":"Pain & Fever","N03":"Neurology","N04":"Neurology",
    "N05":"Sleep & Sedation","N06":"Antidepressants","N07":"Nervous System",
    "P01":"Antiparasitics","P02":"Antiparasitics","P03":"Antiparasitics",
    "R01":"Cough & Cold","R02":"Cough & Cold","R03":"Lungs & Asthma",
    "R04":"Cough & Cold","R05":"Cough & Cold","R06":"Allergy","R07":"Lungs & Asthma",
    "S01":"Eye & Ear","S02":"Eye & Ear","S03":"Eye & Ear",
    "V03":"First Aid","V06":"Vitamins & Supplements","V07":"First Aid","V08":"First Aid",
}

BLACKLIST = re.compile(
    r"\b(vaccin|immunoglobulin|albumin|dialisi|dispositivo|diagnostico|radiofarmac)\b", re.I
)

def atc_category(atc):
    return ATC_MAP.get((atc or "").strip()[:3].upper())

def curl_download(url, dest, max_time=180):
    cmd = ["curl","-L","--max-time",str(max_time),"--connect-timeout","20",
           "--silent","--fail","--user-agent","Mozilla/5.0 apoHouze-updater/5.0",
           "-o", dest, url]
    for attempt in range(3):
        try:
            subprocess.run(cmd, timeout=max_time+15, check=True)
            size = os.path.getsize(dest)
            print(f"  ✅ {size//1024} KB")
            return size
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"  ⚠️  Poging {attempt+1}/3: {e}")
            if attempt < 2: time.sleep(4)
    return 0

def load_ema_json(dest):
    """Laad EMA JSON en retourneer lijst van medicijnen."""
    with open(dest, "r", encoding="utf-8-sig") as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return raw
    for k in ("results","medicines","data","value","items"):
        if k in raw and isinstance(raw[k], list):
            return raw[k]
    return next((v for v in raw.values() if isinstance(v, list)), [])

def process_ema(items, seen):
    results = []
    s = items[0] if items else {}
    keys = list(s.keys())
    def find(*pats):
        for k in keys:
            kl = k.lower().replace(" ","_")
            if any(re.search(p, kl) for p in pats): return k
        return None
    name_key   = find(r"^medicine_name$", r"^name$", r"product")
    inn_key    = find(r"active_substance", r"\binn\b")
    atc_key    = find(r"^atc")
    status_key = find(r"authoris", r"status")
    if DEBUG: print(f"  📋 name:{name_key} atc:{atc_key}")

    sk = 0
    for item in items:
        name   = str(item.get(name_key) or "").strip()
        if not name: continue
        status = str(item.get(status_key) or "") if status_key else ""
        if status and re.search(r"withdrawn|refused|suspended", status, re.I):
            sk += 1; continue
        if BLACKLIST.search(name): sk += 1; continue
        atc  = str(item.get(atc_key) or "").strip()  if atc_key  else ""
        inn  = str(item.get(inn_key) or "").strip()   if inn_key  else ""
        cat  = atc_category(atc)
        if not cat: sk += 1; continue
        key = name.lower()
        if key in seen: continue
        seen.add(key)
        results.append({"Name":name,"INN":inn,"ATC":atc,
                        "PharmaceuticalForm":"","RxStatus":"Rx","Country":"IT"})
    print(f"  ✅ {len(results)} EMA-middelen | {sk} overgeslagen")
    return results

def process_aifa_carenti(path, seen):
    """
    Verwerk AIFA shortage list CSV.
    Kolommen: Nome medicinale;Codice AIC;Principio attivo;Forma farmaceutica e dosaggio;
              Titolare AIC;Data inizio;Fine presunta;Equivalente;Motivazioni;...;Codice ATC
    De laatste kolom is de ATC-code.
    """
    results = []
    # Detecteer encoding
    for enc in ("utf-8-sig", "latin-1", "utf-8"):
        try:
            with open(path, encoding=enc, errors="strict") as f:
                content = f.read()
            break
        except UnicodeDecodeError:
            continue
    else:
        content = open(path, encoding="latin-1", errors="replace").read()

    # Sla header-regels over (beginnen met "NB:" of "Elenco")
    lines = [l for l in content.split("\n") if l.strip() and not l.startswith("NB:") and not l.startswith("Elenco")]
    if not lines:
        return results

    reader = csv.reader(io.StringIO("\n".join(lines)), delimiter=";")
    rows = list(reader)
    if not rows:
        return results

    if DEBUG: print(f"  🔍 AIFA carenti kolommen ({len(rows[0])}): {rows[0][:6]}")
    print(f"  📊 {len(rows)} rijen in AIFA carenti")

    # Zoek kolomindices flexibel
    header = rows[0]
    def col(patterns):
        for i, h in enumerate(header):
            hl = h.lower()
            if any(re.search(p, hl) for p in patterns): return i
        return None

    name_idx = col([r"nome.*medic", r"nome.*farm", r"denominaz"])
    inn_idx  = col([r"principio.*attivo", r"sostanza"])
    form_idx = col([r"forma.*farm"])
    atc_idx  = col([r"codice.*atc", r"\batc\b"])

    # Als geen header, probeer vaste posities (op basis van formaat carenti CSV)
    if name_idx is None: name_idx = 0
    if inn_idx is None:  inn_idx  = 2
    if form_idx is None: form_idx = 3
    if atc_idx is None:  atc_idx  = len(header) - 1  # ATC is vaak de laatste kolom

    data_rows = rows[1:] if any(re.search(r"nome|codice|principio", h, re.I) for h in header) else rows

    sk = 0
    for row in data_rows:
        if len(row) <= max(name_idx, atc_idx if atc_idx is not None else 0): continue
        name = row[name_idx].strip().strip('"') if name_idx is not None else ""
        inn  = row[inn_idx].strip().strip('"')  if inn_idx is not None and inn_idx < len(row) else ""
        form = row[form_idx].strip().strip('"') if form_idx is not None and form_idx < len(row) else ""
        atc  = row[atc_idx].strip().strip('"')  if atc_idx is not None and atc_idx < len(row) else ""

        if not name: continue
        if BLACKLIST.search(name): sk += 1; continue
        cat = atc_category(atc)
        if not cat: sk += 1; continue

        key = name.lower()
        if key not in seen:
            seen.add(key)
            results.append({"Name":name,"INN":inn,"ATC":atc,
                            "PharmaceuticalForm":form,"RxStatus":"Rx","Country":"IT"})
        if inn and inn.lower() != name.lower():
            key2 = inn.lower()
            if key2 not in seen:
                seen.add(key2)
                results.append({"Name":inn,"INN":inn,"ATC":atc,
                                "PharmaceuticalForm":form,"RxStatus":"Rx","Country":"IT"})

    print(f"  ✅ {len(results)} AIFA carenti-middelen | {sk} overgeslagen")
    return results

def save_csv(rows):
    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)
    print(f"\n✅ {len(rows)} medicijnen opgeslagen → {OUTPUT_FILE}")

def main():
    print("🇮🇹 apoHouze — Italië Medicijnen Fetcher v2")
    print("=" * 48)
    print("📌 Bron: EMA JSON + AIFA shortage list\n")

    seen = set()
    all_results = []

    # Bron 1: EMA JSON
    print("[1/4] EMA JSON downloaden...")
    ema_dest = os.path.join(TMP_DIR, "it_ema.json")
    for url in EMA_JSON_URLS:
        print(f"  📥 {url}")
        size = curl_download(url, ema_dest)
        if size > 10_000:
            break
        print(f"  ⚠️  Te klein ({size}B)")

    print("[2/4] EMA verwerken...")
    if os.path.exists(ema_dest) and os.path.getsize(ema_dest) > 10000:
        try:
            items = load_ema_json(ema_dest)
            print(f"  📊 {len(items)} EMA-records")
            r = process_ema(items, seen)
            all_results.extend(r)
        except Exception as e:
            print(f"  ⚠️  EMA verwerking mislukt: {e}")
    else:
        print("  ⚠️  EMA niet beschikbaar, overgeslagen")

    # Bron 2: AIFA carenti CSV (shortage list met Italiaanse namen + ATC)
    print("[3/4] AIFA medicinali carenti downloaden...")
    aifa_dest = os.path.join(TMP_DIR, "it_aifa_carenti.csv")
    print(f"  📥 {AIFA_CARENTI_URL}")
    size = curl_download(AIFA_CARENTI_URL, aifa_dest)
    if size > 1000:
        print("[4/4] AIFA carenti verwerken...")
        try:
            r = process_aifa_carenti(aifa_dest, seen)
            all_results.extend(r)
        except Exception as e:
            print(f"  ⚠️  AIFA carenti verwerking mislukt: {e}")
    else:
        print(f"  ⚠️  AIFA carenti download mislukt ({size}B)")

    print(f"\n  🎯 Totaal: {len(all_results)} unieke medicijnen")

    if not all_results:
        print("❌ Geen resultaten"); sys.exit(1)

    save_csv(all_results)

if __name__ == "__main__":
    main()
