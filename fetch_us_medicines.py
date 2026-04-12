#!/usr/bin/env python3
"""
apoHouze — Verenigde Staten Medicijnen Fetcher v1
==================================================
Bron: openFDA Human Drug NDC Directory (bulk download)
  https://open.fda.gov/data/downloads/

Het openFDA NDC-bestand is een dagelijks bijgewerkte JSON-dump van alle
bij de FDA geregistreerde geneesmiddelen (Rx én OTC).

Relevante velden:
  brand_name      → merknaam
  generic_name    → generieke naam (INN)
  pharm_class     → farmacologische klasse (voor categoriemapping)
  dosage_form     → farmaceutische vorm
  marketing_category → "OTC MONOGRAPH FINAL" e.d. voor Rx/OTC onderscheid
  dea_schedule    → DEA-schedule (CII etc. → Rx)

Output: data/_tmp/us_medicines.csv
  Kolommen: Name,INN,ATC,PharmaceuticalForm,RxStatus,Country

Gebruik: python3 fetch_us_medicines.py [--debug]
"""

import sys, os, re, csv, time, subprocess, json, zipfile, urllib.request

DEBUG = "--debug" in sys.argv
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
TMP_DIR     = os.path.join(SCRIPT_DIR, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "us_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

# openFDA downloads.json geeft altijd de actuele download-URLs
FDA_DOWNLOADS_URL = "https://api.fda.gov/download.json"

# Farmacologische klasse-patronen → apoHouze categorie
# openFDA pharm_class bevat strings als "Proton Pump Inhibitor [EPC]"
PHARM_CLASS_MAP = [
    # Pain & Fever
    (r"analgesic|pain|antipyretic|opioid|nsaid|salicylate", "Pain & Fever"),
    # Antibiotics
    (r"antibacterial|antibiotic|penicillin|cephalosporin|macrolide|tetracycline|fluoroquinolone|aminoglycoside", "Antibiotics"),
    # Antivirals
    (r"antiviral|antiretroviral|neuraminidase", "Antivirals"),
    # Antifungals
    (r"antifungal|azole antifungal|polyene", "Antifungals"),
    # Antiparasitics
    (r"antiparasitic|anthelmintic|antimalarial|antiprotozoal", "Antiparasitics"),
    # Allergy
    (r"antihistamine|histamine.*receptor.*antagonist.*\[epc\]", "Allergy"),
    # Cough & Cold
    (r"decongestant|expectorant|antitussive|nasal", "Cough & Cold"),
    # Lungs & Asthma
    (r"bronchodilator|beta.*agonist.*\[moa\]|corticosteroid.*pulmonary|leukotriene", "Lungs & Asthma"),
    # Stomach & Intestine
    (r"proton pump|antacid|h2.*receptor|laxative|antidiarrheal|antiemetic|prokinetic|gastrointestinal", "Stomach & Intestine"),
    # Heart & Blood Pressure
    (r"antihypertensive|beta.*blocker|ace.*inhibitor|angiotensin|calcium.*channel|diuretic|vasodilator|cardiac", "Heart & Blood Pressure"),
    # Cholesterol
    (r"statin|hmg.coa|lipid|cholesterol", "Cholesterol"),
    # Anticoagulants
    (r"anticoagulant|antiplatelet|thrombolytic|heparin|warfarin", "Anticoagulants"),
    # Diabetes
    (r"antidiabetic|insulin|hypoglycemic|glp.1|sglt|dpp.4", "Diabetes"),
    # Thyroid
    (r"thyroid|antithyroid", "Thyroid"),
    # Corticosteroids
    (r"corticosteroid|glucocorticoid|mineralocorticoid", "Corticosteroids"),
    # Neurology
    (r"anticonvulsant|antiepileptic|anti.parkinson|dopamine|cholinesterase", "Neurology"),
    # Sleep & Sedation
    (r"sedative|hypnotic|anxiolytic|benzodiazepine", "Sleep & Sedation"),
    # Antidepressants
    (r"antidepressant|ssri|snri|maoi|tricyclic antidepressant", "Antidepressants"),
    # Vitamins & Supplements
    (r"vitamin|mineral|supplement|electrolyte|iron|calcium|zinc|magnesium|folic", "Vitamins & Supplements"),
    # Women's Health
    (r"contraceptive|estrogen|progestin|hormone.*replacement|ovulation", "Women's Health"),
    # Urology
    (r"alpha.*blocker.*urolog|benign.*prostate|overactive.*bladder|phosphodiesterase.*\[moa\]", "Urology"),
    # Oncology
    (r"antineoplastic|chemotherapy|cytotoxic|kinase.*inhibitor.*\[moa\]", "Oncology"),
    # Joints & Muscles
    (r"muscle.*relaxant|antigout|uricosuric|bisphosphonate|dmard", "Joints & Muscles"),
    # Skin & Wounds
    (r"topical.*antibiotic|retinoid|keratolytic|emollient|wound", "Skin & Wounds"),
    # Eye & Ear
    (r"ophthalmic|ocular|otic|glaucoma", "Eye & Ear"),
    # First Aid
    (r"anesthetic|antiseptic|disinfectant", "First Aid"),
]

BLACKLIST = re.compile(
    r"\b(vaccine|immunoglobulin|blood|plasma|albumin|diagnostic|"
    r"dressing|device|kit|reagent|contrast|radioactive)\b", re.I
)

def pharm_class_to_category(pharm_classes):
    """Geef apoHouze-categorie op basis van FDA-farmacologische klassen."""
    if not pharm_classes:
        return None
    text = " ".join(pharm_classes).lower()
    for pattern, cat in PHARM_CLASS_MAP:
        if re.search(pattern, text, re.I):
            return cat
    return None


def get_fda_download_url():
    """Haal de actuele download-URL op via openFDA downloads manifest."""
    print(f"  🌐 openFDA downloads manifest ophalen...")
    req = urllib.request.Request(
        FDA_DOWNLOADS_URL,
        headers={"User-Agent": "Mozilla/5.0 apoHouze-updater/5.0"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        manifest = json.loads(r.read())

    # Navigeer naar drug/ndc/partitions
    ndc = manifest.get("results", {}).get("drug", {}).get("ndc", {})
    partitions = ndc.get("partitions", [])

    if not partitions:
        raise RuntimeError("Geen NDC-partities gevonden in manifest")

    # Gebruik de eerste partitie (alle producten zijn in één of meer bestanden)
    url = partitions[0].get("file")
    total = ndc.get("total_records", "?")
    print(f"  ✅ NDC manifest: {len(partitions)} partities, ~{total} records")
    print(f"  🔗 {url}")
    return partitions  # geef alle partities terug


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
            print(f"  ⚠️  Poging {attempt+1}/3: {e}")
            if attempt < 2: time.sleep(5)
    return 0


def process_ndc_partition(path):
    """Verwerk één openFDA NDC JSON-partitie."""
    print(f"  📖 JSON lezen ({os.path.getsize(path)//1024} KB)...")

    # openFDA bestanden zijn gezipt
    if path.endswith(".zip"):
        with zipfile.ZipFile(path, "r") as z:
            names = z.namelist()
            if DEBUG: print(f"  🔍 ZIP inhoud: {names}")
            json_name = next((n for n in names if n.endswith(".json")), names[0])
            with z.open(json_name) as jf:
                data = json.loads(jf.read())
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

    results_list = data.get("results", [])
    print(f"  📊 {len(results_list)} records in partitie")

    rows = []
    sk_bl = 0; sk_cat = 0; sk_name = 0

    for item in results_list:
        brand  = (item.get("brand_name") or "").strip()
        generic = (item.get("generic_name") or "").strip()
        name   = brand or generic
        if not name:
            sk_name += 1; continue

        if BLACKLIST.search(name) or BLACKLIST.search(generic):
            sk_bl += 1; continue

        # Farmacologische klasse voor categorie
        pharm = item.get("pharm_class") or []
        if isinstance(pharm, str): pharm = [pharm]
        category = pharm_class_to_category(pharm)
        if not category:
            sk_cat += 1; continue

        # Rx/OTC bepalen
        mkt_cat = (item.get("marketing_category") or "").upper()
        dea     = (item.get("dea_schedule") or "").strip()
        rx = bool(dea) or ("OTC" not in mkt_cat and "MONOGRAPH" not in mkt_cat)

        form = (item.get("dosage_form") or "").strip()

        rows.append({
            "name": name, "generic": generic,
            "category": category, "form": form, "rx": rx
        })

    print(f"  ✅ {len(rows)} geldig | {sk_cat} geen categorie | {sk_bl} blacklist | {sk_name} geen naam")
    return rows


def save_csv(rows):
    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({
                "Name": r["name"], "INN": r["generic"],
                "ATC": "", "PharmaceuticalForm": r["form"],
                "RxStatus": "Rx" if r["rx"] else "OTC",
                "Country": "US",
            })
    print(f"\n✅ {len(rows)} medicijnen opgeslagen → {OUTPUT_FILE}")


def main():
    print("🇺🇸 apoHouze — Verenigde Staten Medicijnen Fetcher v1")
    print("=" * 54)
    print("📌 Bron: openFDA Human Drug NDC Directory\n")

    print("[1/3] openFDA download-manifest ophalen...")
    try:
        partitions = get_fda_download_url()
    except Exception as e:
        print(f"❌ Manifest ophalen mislukt: {e}"); sys.exit(1)

    all_rows = []
    seen_names = set()

    print(f"\n[2/3] NDC-partities downloaden & verwerken...")
    for i, part in enumerate(partitions):
        url = part.get("file","")
        records = part.get("records", "?")
        print(f"\n  📦 Partitie {i+1}/{len(partitions)} ({records} records)")
        dest = os.path.join(TMP_DIR, f"us_ndc_{i+1}.zip")
        size = curl_download(url, dest)
        if size < 1000:
            print(f"  ⚠️  Overgeslagen (te klein)"); continue
        try:
            rows = process_ndc_partition(dest)
        except Exception as e:
            print(f"  ⚠️  Verwerking mislukt: {e}"); continue

        # Globale deduplicatie over alle partities
        for r in rows:
            k = r["name"].lower()
            if k not in seen_names:
                seen_names.add(k)
                all_rows.append(r)

        # Verwijder partitiebestand direct om schijfruimte te sparen
        try: os.remove(dest)
        except: pass

    print(f"\n  🎯 Totaal uniek: {len(all_rows)} medicijnen")

    if not all_rows:
        print("❌ Geen geldige medicijnen gevonden"); sys.exit(1)

    print(f"\n[3/3] Opslaan...")
    save_csv(all_rows)


if __name__ == "__main__":
    main()
