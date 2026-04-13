#!/usr/bin/env python3
"""
apoHouze — Canada Medicijnen Fetcher v1
========================================
Bron: Health Canada Drug Product Database (DPD) — open.canada.ca
  https://open.canada.ca/data/en/dataset/bf55e42a-63cb-4556-bfd8-44f26e5a36fe

Het DPD bevat ~20.000 goedgekeurde Canadese medicijnen met ATC-codes,
merkname, generieke naam en farmaceutische vorm.

De ZIP (allfiles_ap.zip = goedgekeurde producten) bevat meerdere CSV-bestanden:
  drug.txt          — hoofdproductinfo: DRUG_CODE, BRAND_NAME, CLASS, ...
  ingred.txt        — ingrediënten: DRUG_CODE, INGREDIENT, STRENGTH, ...
  form.txt          — farmaceutische vorm: DRUG_CODE, PHARMACEUTICAL_FORM
  route.txt         — toedieningsweg
  ther.txt          — ATC-codes: DRUG_CODE, TC_ATC_NUMBER, TC_ATC

We joinen drug.txt (merknaam) + ther.txt (ATC) + ingred.txt (INN) + form.txt

Download URLs (stabiel, direct van open.canada.ca):
  Approved products: https://health-products.canada.ca/api/drug/drugproduct/?lang=en&type=json
  ZIP extract:       https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_ap.zip

Output: data/_tmp/ca_medicines.csv
  Kolommen: Name,INN,ATC,PharmaceuticalForm,RxStatus,Country

Gebruik: python3 fetch_ca_medicines.py [--debug]
"""

import sys, os, re, csv, time, subprocess, zipfile, io

DEBUG = "--debug" in sys.argv
# Gebruik os.getcwd() want update.js roept dit script aan met cwd=repo_root
# os.path.dirname(__file__) kan afwijken als Python het pad anders resolvet
REPO_ROOT   = os.getcwd()
TMP_DIR     = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "ca_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

# Health Canada DPD ZIP-bestanden (goedgekeurde producten)
DPD_URLS = [
    # Primair: open.canada.ca open data portal
    "https://open.canada.ca/data/dataset/bf55e42a-63cb-4556-bfd8-44f26e5a36fe/resource/b05ae610-0366-478f-993f-b4afbdaadbc6/download/allfiles.zip",
    # Fallback: directe Health Canada download
    "https://health-products.canada.ca/api/drug/drugproduct/?lang=en&type=zip",
    "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_ap.zip",
]

# ATC-map identiek aan update.js
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
    r"\b(vaccine|vaccin|immunoglobulin|blood|plasma|albumin|diagnostic|"
    r"disinfectant|radiopharmaceutical|veterinary|vet\b|dressing|device)\b", re.I
)


def atc_category(atc):
    return ATC_MAP.get((atc or "").strip()[:3].upper())


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


def read_dpd_csv(zf, filename, col_indices, max_cols=20):
    """
    Lees een DPD-bestand uit de ZIP.
    DPD-bestanden zijn kommagescheiden, dubbel-aangehaald, zonder header.
    Kolommen zijn gedocumenteerd in de readme van Health Canada.
    """
    # Probeer exact bestandsnaam, dan hoofdletteronverschillig
    names = zf.namelist()
    match = None
    for n in names:
        if n.lower().endswith(filename.lower()) or os.path.basename(n).lower() == filename.lower():
            match = n
            break
    if not match:
        if DEBUG: print(f"  ⚠️  {filename} niet gevonden in ZIP. Bestanden: {names[:10]}")
        return {}

    rows = {}
    with zf.open(match) as f:
        content = f.read().decode("utf-8", errors="replace")
        reader = csv.reader(io.StringIO(content))
        for row in reader:
            if len(row) < max(col_indices.values()) + 1:
                continue
            key = row[col_indices["key"]].strip().strip('"')
            vals = {k: row[v].strip().strip('"') for k, v in col_indices.items() if k != "key"}
            if key not in rows:
                rows[key] = vals
            else:
                # Meerdere rijen per key: eerste niet-lege waarde bewaren
                for k, v in vals.items():
                    if v and not rows[key].get(k):
                        rows[key][k] = v
    if DEBUG: print(f"  🔍 {filename}: {len(rows)} unieke keys")
    return rows


def process_dpd_zip(zip_path):
    """
    Verwerk de Health Canada DPD ZIP.

    DPD bestandsstructuur (zonder headers):
    drug.txt:    kolom 0=DRUG_CODE, 1=PRODUCT_CATEGORIZATION, 2=CLASS,
                 3=DRUG_IDENTIFICATION_NUMBER, 4=BRAND_NAME, 5=DESCRIPTOR,
                 6=PEDIATRIC_FLAG, 7=ACCESSION_NUMBER, 8=NUMBER_OF_AIS,
                 9=AI_GROUP_NO, 10=COMPANY_CODE, 11=LAST_UPDATE_DATE
    ther.txt:    kolom 0=DRUG_CODE, 1=TC_ATC_NUMBER, 2=TC_ATC (beschr),
                 3=TC_AHFS_NUMBER, 4=TC_AHFS (beschr.)
    ingred.txt:  kolom 0=DRUG_CODE, 1=ACTIVE_INGREDIENT_CODE, 2=INGREDIENT,
                 3=INGREDIENT_SUPPLIED_IND, 4=STRENGTH, 5=STRENGTH_UNIT,
                 6=STRENGTH_TYPE, 7=DOSAGE_VALUE, 8=BASE, 9=DOSAGE_UNIT,
                 10=NOTES, 11=INGREDIENT_F (FR)
    form.txt:    kolom 0=DRUG_CODE, 1=PHARM_FORM_CODE, 2=PHARMACEUTICAL_FORM,
                 3=PHARMACEUTICAL_FORM_F (FR)
    """
    print(f"  📦 DPD ZIP openen ({os.path.getsize(zip_path)//1024} KB)...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if DEBUG: print(f"  🔍 ZIP inhoud: {names}")

        # drug.txt: merknaam
        drugs = read_dpd_csv(zf, "drug.txt", {
            "key": 0,     # DRUG_CODE
            "brand": 4,   # BRAND_NAME
            "class": 2,   # CLASS (Human/Vet/Disinfectant)
        }, max_cols=12)

        # ther.txt: ATC-code
        thers = read_dpd_csv(zf, "ther.txt", {
            "key": 0,     # DRUG_CODE
            "atc": 1,     # TC_ATC_NUMBER
        }, max_cols=5)

        # ingred.txt: generieke naam
        ingreds = read_dpd_csv(zf, "ingred.txt", {
            "key": 0,        # DRUG_CODE
            "inn": 2,        # INGREDIENT
        }, max_cols=12)

        # form.txt: farmaceutische vorm
        forms = read_dpd_csv(zf, "form.txt", {
            "key": 0,        # DRUG_CODE
            "form": 2,       # PHARMACEUTICAL_FORM
        }, max_cols=4)

    print(f"  📊 {len(drugs)} producten | {len(thers)} met ATC | {len(ingreds)} met INN | {len(forms)} met vorm")

    results = []
    seen = set()
    sk_vet = 0; sk_atc = 0; sk_bl = 0

    for drug_code, drug_info in drugs.items():
        brand = drug_info.get("brand", "").strip()
        cls   = drug_info.get("class", "").strip().upper()

        # Filter veterinaire, desinfectanten, radiofarmaceutica
        if cls in ("V", "D", "R") or "VET" in cls:
            sk_vet += 1; continue
        if not brand or BLACKLIST.search(brand):
            sk_bl += 1; continue

        atc_info = thers.get(drug_code, {})
        atc = atc_info.get("atc", "").strip()
        category = atc_category(atc)
        if not category:
            sk_atc += 1; continue

        inn  = ingreds.get(drug_code, {}).get("inn", "").strip()
        form = forms.get(drug_code, {}).get("form", "").strip()

        key = brand.lower()
        if key in seen: continue
        seen.add(key)

        results.append({
            "Name": brand, "INN": inn, "ATC": atc,
            "PharmaceuticalForm": form, "RxStatus": "Rx", "Country": "CA",
        })

    print(f"  ✅ {len(results)} uniek | {sk_vet} veterinair | {sk_atc} geen ATC | {sk_bl} blacklist")
    return results


def save_csv(rows):
    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)
    print(f"\n✅ {len(rows)} medicijnen opgeslagen → {OUTPUT_FILE}")


def main():
    print("🇨🇦 apoHouze — Canada Medicijnen Fetcher v1")
    print("=" * 48)
    print("📌 Bron: Health Canada Drug Product Database (DPD)\n")

    dest = os.path.join(TMP_DIR, "ca_dpd_allfiles.zip")

    print("[1/3] DPD ZIP downloaden...")
    downloaded = False
    for url in DPD_URLS:
        print(f"  📥 {url}")
        size = curl_download(url, dest)
        if size > 100_000:
            downloaded = True
            break
        print(f"  ⚠️  Te klein ({size}B), volgende URL proberen...")

    if not downloaded:
        print("❌ DPD download mislukt — alle URLs geprobeerd"); sys.exit(1)

    print(f"\n[2/3] DPD verwerken...")
    try:
        results = process_dpd_zip(dest)
    except Exception as e:
        print(f"❌ DPD verwerking mislukt: {e}"); sys.exit(1)

    if not results:
        print("❌ Geen geldige medicijnen gevonden"); sys.exit(1)

    print(f"\n[3/3] Opslaan...")
    save_csv(results)


if __name__ == "__main__":
    main()
