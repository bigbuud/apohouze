#!/usr/bin/env python3
"""
apoHouze — Verenigd Koninkrijk Medicijnen Fetcher v2
=====================================================
Bron: NHSBSA BNF Code Information (Current Year) — via CKAN API
  https://opendata.nhsbsa.net/dataset/bnf-code-information-current-year

Echte BNF CSV-kolommen (verified via NHSBSA documentatie):
  BNF_CODE                     — 15-cijferig BNF-code (eerste 2 = chapter)
  BNF_CHAPTER_DESCR            — chapternaam (bv. "Cardiovascular System")
  BNF_SECTION_CODE             — sectie (4 cijfers)
  BNF_SECTION_DESCR            — sectienaam
  BNF_PARAGRAPH_DESCR          — paragraafnaam
  BNF_SUBPARAGRAPH_DESCR       — subparagraafnaam
  BNF_CHEMICAL_SUBSTANCE_DESCR — generieke stofnaam (= INN-equivalent)
  BNF_PRODUCT_DESCR            — productnaam (merknaam of generiek)
  BNF_PRESENTATION_DESCR       — volledige presentatienaam incl. sterkte/vorm
  UNIT_OF_MEASURE              — eenheid

We gebruiken BNF_PRESENTATION_DESCR als naam en BNF_SECTION_CODE voor
categoriemapping (eerste 4 cijfers van de code).

Output: data/_tmp/gb_medicines.csv
  Kolommen: Name,INN,ATC,PharmaceuticalForm,RxStatus,Country

Gebruik: python3 fetch_gb_medicines.py [--debug]
"""

import sys, os, re, csv, time, subprocess, json, urllib.request
from urllib.parse import urlencode

DEBUG = "--debug" in sys.argv
# Gebruik os.getcwd() want update.js roept dit script aan met cwd=repo_root
# os.path.dirname(__file__) kan afwijken als Python het pad anders resolvet
REPO_ROOT   = os.getcwd()
TMP_DIR     = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "gb_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

CKAN_BASE    = "https://opendata.nhsbsa.net/api/3/action"
DATASET_IDS  = [
    "bnf-code-information-current-year",
    "bnf-code-information-historic",
]

# BNF sectie (4 cijfers) → apoHouze categorie
# Gebaseerd op BNF hoofdstukken 1-15 met sectie-overrides
BNF_SECTION_MAP = {
    # Hoofdstuk 01: Maag & Darmen
    "0101": "Stomach & Intestine", "0102": "Stomach & Intestine",
    "0103": "Stomach & Intestine", "0104": "Stomach & Intestine",
    "0105": "Stomach & Intestine", "0106": "Stomach & Intestine",
    "0107": "Stomach & Intestine", "0108": "Stomach & Intestine",
    "0109": "Stomach & Intestine",
    # Hoofdstuk 02: Hart & Vaatstelsel
    "0201": "Heart & Blood Pressure", "0202": "Heart & Blood Pressure",
    "0203": "Heart & Blood Pressure", "0204": "Heart & Blood Pressure",
    "0205": "Heart & Blood Pressure", "0206": "Heart & Blood Pressure",
    "0207": "Heart & Blood Pressure",
    "0208": "Anticoagulants", "0209": "Anticoagulants",
    "0210": "Cholesterol", "0211": "Heart & Blood Pressure",
    "0212": "Cholesterol",
    # Hoofdstuk 03: Luchtwegen
    "0301": "Lungs & Asthma", "0302": "Lungs & Asthma",
    "0303": "Allergy",
    "0304": "Cough & Cold", "0305": "Cough & Cold",
    # Hoofdstuk 04: Zenuwstelsel
    "0401": "Sleep & Sedation",
    "0402": "Antidepressants", "0403": "Antidepressants",
    "0404": "Sleep & Sedation",
    "0405": "Neurology", "0406": "Neurology",
    "0407": "Pain & Fever", "0408": "Pain & Fever", "0409": "Pain & Fever",
    "0410": "Neurology", "0411": "Neurology",
    # Hoofdstuk 05: Infecties
    "0501": "Antibiotics", "0502": "Antifungals",
    "0503": "Antivirals", "0504": "Antiparasitics",
    "0505": "Antiparasitics",
    # Hoofdstuk 06: Endocrien
    "0601": "Diabetes", "0602": "Thyroid",
    "0603": "Corticosteroids",
    "0604": "Women's Health", "0605": "Thyroid",
    "0606": "Women's Health", "0607": "Urology",
    "0608": "Women's Health", "0609": "Vitamins & Supplements",
    # Hoofdstuk 07: Genitourinair
    "0701": "Women's Health", "0702": "Women's Health",
    "0703": "Women's Health", "0704": "Urology",
    "0705": "Urology",
    # Hoofdstuk 08: Oncologie
    "0801": "Oncology", "0802": "Oncology",
    "0803": "Oncology",
    # Hoofdstuk 09: Voeding & Bloed
    "0901": "Vitamins & Supplements", "0902": "Vitamins & Supplements",
    "0903": "Vitamins & Supplements", "0904": "Vitamins & Supplements",
    "0905": "Vitamins & Supplements", "0906": "Vitamins & Supplements",
    # Hoofdstuk 10: Spier & Bot
    "1001": "Pain & Fever", "1002": "Joints & Muscles",
    "1003": "Joints & Muscles", "1004": "Joints & Muscles",
    # Hoofdstuk 11: Oog
    "1101": "Eye & Ear", "1102": "Eye & Ear",
    "1103": "Eye & Ear", "1104": "Eye & Ear",
    "1105": "Eye & Ear",
    # Hoofdstuk 12: Oor, Neus, Orofarynx
    "1201": "Cough & Cold", "1202": "Eye & Ear",
    "1203": "Cough & Cold",
    # Hoofdstuk 13: Huid
    "1301": "Skin & Wounds", "1302": "Skin & Wounds",
    "1303": "Skin & Wounds", "1304": "Corticosteroids",
    "1305": "Skin & Wounds", "1306": "Antifungals",
    "1307": "Skin & Wounds", "1308": "Skin & Wounds",
    "1309": "Skin & Wounds", "1310": "Skin & Wounds",
    # Hoofdstuk 14: Vaccinaties
    "1401": "Antivirals", "1402": "Antivirals",
    "1403": "Antivirals", "1404": "Antivirals",
    # Hoofdstuk 15: Anaesthetica
    "1501": "First Aid", "1502": "First Aid",
    "1503": "First Aid", "1504": "First Aid",
}

APPLIANCE_BLACKLIST = re.compile(
    r"\b(dressing|catheter|bandage|stoma|bag|pad|syringe|needle|lancet|"
    r"strip|monitor|machine|pump|splint|brace|glove|mask|suture|staple|"
    r"wound|incontinence|colostomy|ileostomy|tracheostomy)\b", re.I
)


def bnf_code_to_category(bnf_code):
    """BNF-code (15 cijfers) → categorie via sectie (4 cijfers)."""
    if not bnf_code or len(bnf_code) < 4:
        return None
    # Pseudo-hoofdstukken 19-23: dressings/appliances → skip
    chapter = bnf_code[:2]
    if chapter in ("19","20","21","22","23"):
        return None
    section = bnf_code[:4]
    return BNF_SECTION_MAP.get(section)


def ckan_api(endpoint, params=None):
    """CKAN API aanroep."""
    url = f"{CKAN_BASE}/{endpoint}"
    if params:
        url += "?" + urlencode(params)
    if DEBUG: print(f"  🌐 {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 apoHouze-updater/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    if not data.get("success"):
        raise RuntimeError(f"CKAN API fout: {data.get('error')}")
    return data["result"]


def get_csv_url():
    """Haal de meest recente CSV-URL op via CKAN API."""
    for dataset_id in DATASET_IDS:
        try:
            print(f"  🔍 CKAN: {dataset_id}")
            result = ckan_api("package_show", {"id": dataset_id})
            resources = result.get("resources", [])
            # Filter op CSV, neem de eerste (meest recent)
            for r in resources:
                url = r.get("url","")
                fmt = r.get("format","").upper()
                if fmt == "CSV" or url.lower().endswith(".csv"):
                    print(f"  ✅ Resource: {r.get('name','?')}")
                    print(f"  🔗 {url}")
                    return url
        except Exception as e:
            print(f"  ⚠️  {dataset_id}: {e}")
    raise RuntimeError("Geen CSV gevonden via CKAN API")


def curl_download(url, dest, max_time=300):
    cmd = [
        "curl", "-L", "--max-time", str(max_time), "--connect-timeout", "20",
        "--silent", "--fail",
        "--user-agent", "Mozilla/5.0 apoHouze-updater/5.0",
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
            if attempt < 2: time.sleep(5)
    return 0


def process_bnf_csv(path):
    """
    Verwerk NHSBSA BNF CSV.

    Echte kolomnamen (lowercase gezocht):
      BNF_CODE                     → 15-cijferig code
      BNF_PRESENTATION_DESCR       → volledige naam (bv. "Amoxicillin 500mg capsules")
      BNF_CHEMICAL_SUBSTANCE_DESCR → generieke naam (bv. "Amoxicillin")
      BNF_PRODUCT_DESCR            → productnaam
    """
    print(f"  📖 CSV lezen...")
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        sample = f.read(8192)
        f.seek(0)
        sep = "\t" if sample.count("\t") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=sep)
        rows = list(reader)

    if not rows:
        raise RuntimeError("CSV is leeg")

    # Normaliseer kolomnamen
    def norm(d):
        return {k.strip().upper(): v for k, v in d.items()}
    rows = [norm(r) for r in rows]

    if DEBUG and rows:
        print(f"  🔍 Kolomnamen: {list(rows[0].keys())}")
    else:
        print(f"  📊 {len(rows)} rijen | Kolommen: {list(rows[0].keys())[:6]}")

    results = []
    skipped_bl  = 0
    skipped_cat = 0
    seen        = set()

    for row in rows:
        # Flexibele kolomzoekstrategie — meerdere mogelijke namen
        code = (row.get("BNF_CODE") or row.get("BNFCODE") or
                row.get("BNF CODE") or "").strip()

        # Naam: voorkeur voor volledige presentatienaam
        name = (row.get("BNF_PRESENTATION_DESCR") or
                row.get("BNF_PRODUCT_DESCR") or
                row.get("BNF_PRESENTATION_NAME") or
                row.get("BNF_DESCRIPTION") or
                row.get("PRESENTATION_DESCR") or "").strip()

        inn  = (row.get("BNF_CHEMICAL_SUBSTANCE_DESCR") or
                row.get("CHEMICAL_SUBSTANCE_DESCR") or
                row.get("BNF_CHEMICAL_SUBSTANCE") or "").strip()

        if not name or not code:
            continue

        # Filter hulpmiddelen
        if APPLIANCE_BLACKLIST.search(name):
            skipped_bl += 1
            continue

        category = bnf_code_to_category(code)
        if not category:
            skipped_cat += 1
            continue

        # Deduplicatie op naam
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)

        results.append({
            "Name":               name,
            "INN":                inn,
            "ATC":                code[:7] if code else "",
            "PharmaceuticalForm": "",
            "RxStatus":           "Rx",
            "Country":            "GB",
        })

    print(f"  ✅ {len(results)} unieke medicijnen | {skipped_cat} geen categorie | {skipped_bl} blacklist")
    return results


def save_csv(rows):
    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)
    print(f"\n✅ {len(rows)} medicijnen opgeslagen → {OUTPUT_FILE}")


def main():
    print("🇬🇧 apoHouze — Verenigd Koninkrijk Medicijnen Fetcher v2")
    print("=" * 56)
    print("📌 Bron: NHSBSA BNF Code Information (CKAN open data)\n")

    print("[1/3] CSV-URL ophalen via CKAN API...")
    try:
        csv_url = get_csv_url()
    except Exception as e:
        print(f"❌ CKAN API mislukt: {e}"); sys.exit(1)

    dest = os.path.join(TMP_DIR, "gb_bnf_raw.csv")
    print(f"\n[2/3] CSV downloaden...")
    size = curl_download(csv_url, dest)
    if size < 5_000:
        print(f"❌ Download mislukt of bestand te klein ({size}B)"); sys.exit(1)

    print(f"\n[3/3] Verwerken & opslaan...")
    try:
        results = process_bnf_csv(dest)
    except Exception as e:
        print(f"❌ CSV verwerking mislukt: {e}"); sys.exit(1)

    if not results:
        print("❌ Geen geldige medicijnen na filtering"); sys.exit(1)

    save_csv(results)


if __name__ == "__main__":
    main()
