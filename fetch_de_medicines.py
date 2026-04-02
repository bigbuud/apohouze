#!/usr/bin/env python3
"""
apoHouze — Duitsland Medicijnen Fetcher v3
==========================================
Bron: EMA JSON API (europa.eu) — gecentraliseerde EU-vergunningen

De EMA XLSX wordt geblokkeerd door CDN/WAF vanuit CI-omgevingen.
Het JSON-rapport op europa.eu is publiek en robuust bereikbaar.

URL: https://www.ema.europa.eu/en/medicines/download-medicine-data
JSON: medicines-output-medicines_json-report_en.json

Output: data/_tmp/de_medicines.csv
  Kolommen: Name,INN,ATC,PharmaceuticalForm,RxStatus,Country

Gebruik: python3 fetch_de_medicines.py [--debug]
"""

import sys, os, re, csv, time, subprocess, json, urllib.request

DEBUG = "--debug" in sys.argv
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
TMP_DIR     = os.path.join(SCRIPT_DIR, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "de_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

# EMA publiceert het JSON-rapport op dezelfde pagina als de XLSX,
# maar via europa.eu/data — toegankelijker vanuit CI-omgevingen.
EMA_JSON_URLS = [
    # Primair: EMA JSON rapport (europa.eu open data)
    "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json",
    # Fallback: data.europa.eu (EU open data portaal mirror)
    "https://data.europa.eu/api/hub/search/datasets/epar-human-medicines?format=json",
]

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

BLACKLIST = re.compile(r"\b(device|diagnostic|kit|test|imaging|dressing|appliance)\b", re.I)
WITHDRAWN = re.compile(r"withdrawn|refused|suspended|expired|revoked", re.I)


def atc_category(atc):
    return ATC_MAP.get((atc or "").strip()[:3].upper())


def curl_get(url, dest, max_time=120):
    """Download via curl met meerdere retry's."""
    cmd = [
        "curl", "-L", "--max-time", str(max_time), "--connect-timeout", "20",
        "--silent", "--fail",
        "--user-agent", "Mozilla/5.0 apoHouze-updater/5.0",
        "-H", "Accept: application/json, */*",
        "-o", dest, url,
    ]
    for attempt in range(3):
        try:
            subprocess.run(cmd, timeout=max_time + 15, check=True)
            size = os.path.getsize(dest)
            print(f"  ✅ {size // 1024} KB gedownload")
            return size
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"  ⚠️  Poging {attempt+1}/3 mislukt: {e}")
            if attempt < 2:
                time.sleep(5)
    return 0


def fetch_ema_json():
    """
    Haal EMA medicijnenlijst op als JSON.
    Het JSON-bestand is hetzelfde rapport als de XLSX maar in JSON-formaat.
    Structuur: lijst van objecten met velden zoals:
      "Medicine name", "Active substance", "ATC code",
      "Authorisation status", "Pharmaceutical form"
    """
    dest = os.path.join(TMP_DIR, "ema_medicines.json")

    for url in EMA_JSON_URLS:
        print(f"  📥 {url}")
        size = curl_get(url, dest)
        if size > 10_000:
            break
    else:
        # Laatste kans: urllib direct
        print("  🔄 urllib fallback...")
        try:
            req = urllib.request.Request(
                EMA_JSON_URLS[0],
                headers={"User-Agent": "Mozilla/5.0 apoHouze-updater/5.0",
                         "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=60) as r, open(dest, "wb") as f:
                data = r.read()
                f.write(data)
            size = len(data)
            print(f"  ✅ {size // 1024} KB via urllib")
        except Exception as e:
            print(f"  ❌ urllib ook mislukt: {e}")
            return []

    try:
        with open(dest, "r", encoding="utf-8-sig") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  ❌ JSON parse mislukt: {e}")
        return []

    # Het JSON-rapport is een lijst van dicts of genest onder een sleutel
    if isinstance(raw, list):
        medicines = raw
    elif isinstance(raw, dict):
        # Zoek de lijst op
        for key in ("results", "medicines", "data", "value", "items"):
            if key in raw and isinstance(raw[key], list):
                medicines = raw[key]
                break
        else:
            # Neem de eerste lijstwaarde
            medicines = next((v for v in raw.values() if isinstance(v, list)), [])

    print(f"  📊 {len(medicines)} records geladen")
    if DEBUG and medicines:
        print(f"  🔍 Sleutels voorbeeld: {list(medicines[0].keys())[:10]}")
    return medicines


def process_ema(medicines):
    if not medicines:
        return []

    sample = medicines[0]
    keys = list(sample.keys())

    def find(patterns):
        for k in keys:
            kl = k.lower().replace(" ", "_")
            if any(re.search(p, kl) for p in patterns):
                return k
        return None

    name_key   = find([r"^medicine_name$", r"^medicine$", r"^name$", r"product"])
    inn_key    = find([r"active_substance", r"\binn\b", r"generic"])
    atc_key    = find([r"^atc"])
    form_key   = find([r"pharmaceutical_form", r"\bform\b"])
    status_key = find([r"authoris", r"status"])

    print(f"  📋 name:{name_key} | inn:{inn_key} | atc:{atc_key} | form:{form_key}")

    if not name_key:
        print(f"  ❌ Naamkolom niet gevonden. Beschikbare sleutels: {keys[:12]}")
        return []

    results, sk_status, sk_atc, sk_bl = [], 0, 0, 0
    for med in medicines:
        name   = str(med.get(name_key) or "").strip()
        if not name: continue
        status = str(med.get(status_key) or "") if status_key else ""
        if status and WITHDRAWN.search(status): sk_status += 1; continue
        if BLACKLIST.search(name): sk_bl += 1; continue
        atc  = str(med.get(atc_key) or "").strip()  if atc_key  else ""
        inn  = str(med.get(inn_key) or "").strip()   if inn_key  else ""
        form = str(med.get(form_key) or "").strip()  if form_key else ""
        if not atc_category(atc): sk_atc += 1; continue
        results.append({"Name": name, "INN": inn, "ATC": atc,
                        "PharmaceuticalForm": form, "RxStatus": "Rx", "Country": "EU"})

    print(f"  ✅ {len(results)} geldig | {sk_status} ingetrokken | {sk_atc} geen ATC | {sk_bl} blacklist")
    return results


def deduplicate(rows):
    seen, out = set(), []
    for r in rows:
        k = r["Name"].lower()
        if k not in seen:
            seen.add(k); out.append(r)
    print(f"  🎯 Na dedup: {len(out)} unieke medicijnen")
    return out


def save_csv(rows):
    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)
    print(f"\n✅ {len(rows)} medicijnen opgeslagen → {OUTPUT_FILE}")


def main():
    print("🇩🇪 apoHouze — Duitsland Medicijnen Fetcher v3")
    print("=" * 52)
    print("📌 Bron: EMA JSON rapport (europa.eu open data)\n")

    print("[1/3] EMA JSON ophalen...")
    medicines = fetch_ema_json()
    if not medicines:
        print("❌ Geen data van EMA JSON"); sys.exit(1)

    print("\n[2/3] Verwerken...")
    processed = process_ema(medicines)
    deduped   = deduplicate(processed)
    if not deduped:
        print("❌ Geen geldige medicijnen na filtering"); sys.exit(1)

    print("\n[3/3] Opslaan...")
    save_csv(deduped)


if __name__ == "__main__":
    main()
