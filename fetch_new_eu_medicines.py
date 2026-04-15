#!/usr/bin/env python3
"""
apoHouze — Nieuwe EU-landen Medicijnen Fetcher
================================================
Gebruik: python3 fetch_new_eu_medicines.py <landcode>

Landen: CZ, SK, HR, SI, HU, RO, GR, LU

Strategie:
  Alle landen: EMA JSON als basis (gecentraliseerde EU-vergunningen)
  CZ: SÚKL open data (opendata.sukl.cz)
  SK: ŠÚKL register (sukl.sk)
  HR: HALMED register (halmed.hr)
  SI: JAZMP register (jazmp.si)
  HU: OGYÉI register (ogyei.gov.hu)
  RO: ANMDMR register (anm.ro)
  GR: EOF register (eof.gr)
  LU: SAM CIVICS BE (zelfde infrastructuur als België)

Output: data/_tmp/<code>_medicines.csv  (zelfde patroon als fetch_eu_medicines.py)
"""

import sys, os, csv, json, io, time
import urllib.request, urllib.error, zipfile

if len(sys.argv) < 2:
    print(f"Gebruik: python3 {sys.argv[0]} <landcode>"); sys.exit(1)

COUNTRY   = sys.argv[1].upper()
REPO_ROOT = os.getcwd()
TMP_DIR   = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT    = os.path.join(TMP_DIR, f"{COUNTRY.lower()}_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

# ================================================================
# ATC → categorie mapping (zelfde als rest van apoHouze)
# ================================================================
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

def atc_to_category(atc):
    if not atc: return ""
    return ATC_MAP.get(atc.strip()[:3].upper(), "")

def http_get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "apoHouze-updater/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def write_csv(rows):
    """Schrijf resultaten naar CSV in het formaat dat update.js verwacht."""
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name","generic","atc","pharmaceutical_form","rx","status"])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    size = os.path.getsize(OUTPUT)
    print(f"  ✅ CSV geschreven: {OUTPUT} ({size//1024} KB, {len(rows)} rijen)")

# ================================================================
# STAP 1: EMA JSON — basis voor alle EU-landen
# ================================================================
def fetch_ema():
    print("  📥 EMA JSON ophalen (gecentraliseerde EU-vergunningen)...")
    url = "https://www.ema.europa.eu/sites/default/files/Medicines_output_european_public_assessment_reports.xlsx"
    # Fallback naar bekende JSON endpoint
    json_url = "https://www.ema.europa.eu/en/medicines/download-medicine-data"
    rows = []
    try:
        data = http_get("https://raw.githubusercontent.com/datasets/medicine-approvals/main/data/ema.csv", timeout=30)
        reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
        for row in reader:
            name    = row.get("Medicine name","").strip()
            inn     = row.get("Active substance","").strip()
            atc     = row.get("ATC code","").strip()
            form    = row.get("Pharmaceutical form","").strip()
            status  = row.get("Status","").strip()
            if not name or "withdrawn" in status.lower():
                continue
            rows.append({"name": name, "generic": inn, "atc": atc,
                         "pharmaceutical_form": form, "rx": "Rx", "status": status})
        print(f"    EMA: {len(rows)} records")
    except Exception as e:
        print(f"    EMA JSON niet beschikbaar: {e}")
    return rows

# ================================================================
# STAP 2: Landsspecifieke bronnen
# ================================================================

def fetch_cz():
    """SÚKL open data — opendata.sukl.cz"""
    print("  📥 SÚKL (CZ) open data ophalen...")
    rows = []
    try:
        # SÚKL biedt ZIP met CSV
        urls_to_try = [
            "https://opendata.sukl.cz/soubory/DLP.zip",
            "https://opendata.sukl.cz/soubory/registry.zip",
        ]
        data = None
        for url in urls_to_try:
            try:
                data = http_get(url, timeout=90)
                print(f"    Downloaded van {url}")
                break
            except: continue

        if not data:
            print("    SÚKL ZIP niet beschikbaar — alleen EMA-data")
            return rows

        with zipfile.ZipFile(io.BytesIO(data)) as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
            print(f"    ZIP bevat: {csv_files}")
            for csv_name in csv_files[:1]:  # eerste CSV
                with z.open(csv_name) as f:
                    content = f.read().decode("utf-8-sig", errors="replace")
                    reader = csv.DictReader(io.StringIO(content), delimiter=";")
                    for row in reader:
                        # Kolommen: pas aan na inspectie van het echte bestand
                        name   = str(row.get("NAZEV","") or row.get("Název","") or "").strip()
                        inn    = str(row.get("INN","") or row.get("Účinná látka","") or "").strip()
                        atc    = str(row.get("ATC","") or "").strip()
                        form   = str(row.get("FORMA","") or row.get("Léková forma","") or "").strip()
                        status = str(row.get("STAV","") or "").strip().lower()
                        if not name or "zrušen" in status:
                            continue
                        rows.append({"name": name, "generic": inn, "atc": atc,
                                     "pharmaceutical_form": form, "rx": "Rx", "status": "authorised"})
        print(f"    SÚKL: {len(rows)} records")
    except Exception as e:
        print(f"    SÚKL fout: {e}")
    return rows


def fetch_sk():
    """ŠÚKL register — sukl.sk"""
    print("  📥 ŠÚKL (SK) register ophalen...")
    rows = []
    try:
        import openpyxl
        urls_to_try = [
            "https://www.sukl.sk/buxus/docs/lieky/register_liekov.xlsx",
            "https://www.sukl.sk/main.php?lng=sk&page=74",
        ]
        data = None
        for url in urls_to_try:
            try:
                data = http_get(url, timeout=90)
                if len(data) > 10000:
                    break
            except: continue

        if not data or len(data) < 10000:
            print("    ŠÚKL Excel niet beschikbaar")
            return rows

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        ws = wb.active
        headers = None
        for row in ws.iter_rows(values_only=True):
            if headers is None:
                headers = [str(c or "").strip() for c in row]
                print(f"    Kolommen: {headers[:8]}")
                continue
            rd = dict(zip(headers, row))
            name  = str(rd.get("Názov lieku","") or rd.get("Obchodný názov","") or "").strip()
            inn   = str(rd.get("INN","") or rd.get("Účinná látka","") or "").strip()
            atc   = str(rd.get("ATC","") or "").strip()
            form  = str(rd.get("Lieková forma","") or "").strip()
            if not name: continue
            rows.append({"name": name, "generic": inn, "atc": atc,
                         "pharmaceutical_form": form, "rx": "Rx", "status": "authorised"})
        print(f"    ŠÚKL: {len(rows)} records")
    except ImportError:
        print("    openpyxl niet geïnstalleerd — pip install openpyxl")
    except Exception as e:
        print(f"    ŠÚKL fout: {e}")
    return rows


def fetch_hr():
    """HALMED register — halmed.hr"""
    print("  📥 HALMED (HR) register ophalen...")
    rows = []
    try:
        import openpyxl
        urls_to_try = [
            "https://www.halmed.hr/lijekovi/baza-lijekova/preuzimanje/",
            "https://www.halmed.hr/fdsak38ds/lijekovi.xlsx",
        ]
        data = None
        for url in urls_to_try:
            try:
                data = http_get(url, timeout=90)
                if len(data) > 10000: break
            except: continue

        if not data or len(data) < 10000:
            print("    HALMED Excel niet beschikbaar")
            return rows

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        ws = wb.active
        headers = None
        for row in ws.iter_rows(values_only=True):
            if headers is None:
                headers = [str(c or "").strip() for c in row]
                print(f"    Kolommen: {headers[:8]}")
                continue
            rd = dict(zip(headers, row))
            name = str(rd.get("Naziv lijeka","") or rd.get("Ime lijeka","") or "").strip()
            inn  = str(rd.get("INN","") or rd.get("Djelatna tvar","") or "").strip()
            atc  = str(rd.get("ATC","") or "").strip()
            form = str(rd.get("Farmaceutski oblik","") or "").strip()
            if not name: continue
            rows.append({"name": name, "generic": inn, "atc": atc,
                         "pharmaceutical_form": form, "rx": "Rx", "status": "authorised"})
        print(f"    HALMED: {len(rows)} records")
    except Exception as e:
        print(f"    HALMED fout: {e}")
    return rows


def fetch_si():
    """JAZMP register — jazmp.si"""
    print("  📥 JAZMP (SI) register ophalen...")
    rows = []
    try:
        import openpyxl
        urls_to_try = [
            "https://www.jazmp.si/fileadmin/datoteke/baza_zdravil/baza_zdravil.xlsx",
            "https://www.jazmp.si/humana-zdravila/baza-podatkov-o-zdravilih/",
        ]
        data = None
        for url in urls_to_try:
            try:
                data = http_get(url, timeout=90)
                if len(data) > 10000: break
            except: continue

        if not data or len(data) < 10000:
            print("    JAZMP Excel niet beschikbaar")
            return rows

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        ws = wb.active
        headers = None
        for row in ws.iter_rows(values_only=True):
            if headers is None:
                headers = [str(c or "").strip() for c in row]
                print(f"    Kolommen: {headers[:8]}")
                continue
            rd = dict(zip(headers, row))
            name = str(rd.get("Ime zdravila","") or rd.get("Naziv","") or "").strip()
            inn  = str(rd.get("INN","") or rd.get("Učinkovina","") or "").strip()
            atc  = str(rd.get("ATC","") or "").strip()
            form = str(rd.get("Farmacevtska oblika","") or "").strip()
            if not name: continue
            rows.append({"name": name, "generic": inn, "atc": atc,
                         "pharmaceutical_form": form, "rx": "Rx", "status": "authorised"})
        print(f"    JAZMP: {len(rows)} records")
    except Exception as e:
        print(f"    JAZMP fout: {e}")
    return rows


def fetch_hu():
    """OGYÉI register — ogyei.gov.hu"""
    print("  📥 OGYÉI (HU) register ophalen...")
    rows = []
    try:
        import openpyxl
        urls_to_try = [
            "https://www.ogyei.gov.hu/gyogyszeradatbazis/download/engedely.xlsx",
            "https://www.ogyei.gov.hu/gyogyszer_adatbazis/",
        ]
        data = None
        for url in urls_to_try:
            try:
                data = http_get(url, timeout=90)
                if len(data) > 10000: break
            except: continue

        if not data or len(data) < 10000:
            print("    OGYÉI Excel niet beschikbaar")
            return rows

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        ws = wb.active
        headers = None
        for row in ws.iter_rows(values_only=True):
            if headers is None:
                headers = [str(c or "").strip() for c in row]
                print(f"    Kolommen: {headers[:8]}")
                continue
            rd = dict(zip(headers, row))
            name = str(rd.get("Készítmény neve","") or rd.get("Gyógyszer neve","") or "").strip()
            inn  = str(rd.get("INN","") or rd.get("Hatóanyag","") or "").strip()
            atc  = str(rd.get("ATC","") or "").strip()
            form = str(rd.get("Gyógyszerforma","") or "").strip()
            if not name: continue
            rows.append({"name": name, "generic": inn, "atc": atc,
                         "pharmaceutical_form": form, "rx": "Rx", "status": "authorised"})
        print(f"    OGYÉI: {len(rows)} records")
    except Exception as e:
        print(f"    OGYÉI fout: {e}")
    return rows


def fetch_ro():
    """ANMDMR register — anm.ro"""
    print("  📥 ANMDMR (RO) register ophalen...")
    rows = []
    try:
        import openpyxl
        urls_to_try = [
            "https://anm.ro/wp-content/uploads/lista_medicamente.xlsx",
            "https://anm.ro/medicamente/medicamente-autorizate/",
        ]
        data = None
        for url in urls_to_try:
            try:
                data = http_get(url, timeout=90)
                if len(data) > 10000: break
            except: continue

        if not data or len(data) < 10000:
            print("    ANMDMR Excel niet beschikbaar")
            return rows

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        ws = wb.active
        headers = None
        for row in ws.iter_rows(values_only=True):
            if headers is None:
                headers = [str(c or "").strip() for c in row]
                print(f"    Kolommen: {headers[:8]}")
                continue
            rd = dict(zip(headers, row))
            # RO: DCI = Denumire Comună Internaţională = INN
            name = str(rd.get("Denumire comerciala","") or rd.get("Denumire","") or "").strip()
            inn  = str(rd.get("DCI","") or rd.get("Substanta activa","") or "").strip()
            atc  = str(rd.get("Cod ATC","") or rd.get("ATC","") or "").strip()
            form = str(rd.get("Forma farmaceutica","") or "").strip()
            if not name: continue
            rows.append({"name": name, "generic": inn, "atc": atc,
                         "pharmaceutical_form": form, "rx": "Rx", "status": "authorised"})
        print(f"    ANMDMR: {len(rows)} records")
    except Exception as e:
        print(f"    ANMDMR fout: {e}")
    return rows


def fetch_gr():
    """EOF register — eof.gr"""
    print("  📥 EOF (GR) register ophalen...")
    rows = []
    try:
        import openpyxl
        urls_to_try = [
            "https://www.eof.gr/web/guest/eofapproved2",
            "https://www.eof.gr/c/document_library/get_file?uuid=approved_medicines.xlsx",
        ]
        data = None
        for url in urls_to_try:
            try:
                data = http_get(url, timeout=90)
                if len(data) > 10000: break
            except: continue

        if not data or len(data) < 10000:
            print("    EOF Excel niet beschikbaar — Griekenland valt terug op EMA-data")
            return rows

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        ws = wb.active
        headers = None
        for row in ws.iter_rows(values_only=True):
            if headers is None:
                headers = [str(c or "").strip() for c in row]
                print(f"    Kolommen: {headers[:8]}")
                continue
            rd = dict(zip(headers, row))
            # GR kolommen kunnen Grieks of Engels zijn
            name = str(rd.get("Εμπορική Ονομασία","") or rd.get("Trade name","") or rd.get("Name","") or "").strip()
            inn  = str(rd.get("INN","") or rd.get("Δραστική Ουσία","") or "").strip()
            atc  = str(rd.get("ATC","") or "").strip()
            form = str(rd.get("Φαρμακοτεχνική Μορφή","") or rd.get("Pharmaceutical form","") or "").strip()
            if not name: continue
            rows.append({"name": name, "generic": inn, "atc": atc,
                         "pharmaceutical_form": form, "rx": "Rx", "status": "authorised"})
        print(f"    EOF: {len(rows)} records")
    except Exception as e:
        print(f"    EOF fout: {e}")
    return rows


def fetch_lu():
    """
    Luxemburg gebruikt grotendeels de Belgische SAM-infrastructuur.
    Haalt BE-data op en filtert/markeert als LU.
    LU-specifieke merknamen (FR + DE) worden toegevoegd.
    """
    print("  📥 LU — SAM CIVICS BE ophalen (LU gebruikt BE-infrastructuur)...")
    rows = []

    # LU-specifieke bekende merknamen (zowel FR als DE variant)
    lu_brands = [
        ("Doliprane Comprimé 500mg",       "Paracetamol",    "N02BE01", "Tablet",       False),
        ("Doliprane Comprimé 1000mg",      "Paracetamol",    "N02BE01", "Tablet",       False),
        ("Efferalgan Comprimé 500mg",      "Paracetamol",    "N02BE01", "Effervescent tablet", False),
        ("Dafalgan Comprimé 1000mg",       "Paracetamol",    "N02BE01", "Tablet",       False),
        ("Aspirine UPSA 500mg",            "Aspirin",        "N02BA01", "Effervescent tablet", False),
        ("Ben-u-ron Tablette 500mg",       "Paracetamol",    "N02BE01", "Tablet",       False),
        ("Thomapyrin Tablette",            "Paracetamol+ASS","N02BA51", "Tablet",       False),
        ("Nurofen Comprimé 200mg",         "Ibuprofen",      "M01AE01", "Tablet",       False),
        ("Nurofen Comprimé 400mg",         "Ibuprofen",      "M01AE01", "Tablet",       False),
        ("Voltarène Emulgel 1%",           "Diclofenac",     "M02AA15", "Gel",          False),
        ("Otrivine Spray nasal 0.1%",      "Xylometazoline", "R01AA07", "Nasal spray",  False),
        ("Zyrtec Comprimé 10mg",           "Cetirizine",     "R06AE07", "Tablet",       False),
        ("Clarityne Comprimé 10mg",        "Loratadine",     "R06AX13", "Tablet",       False),
        ("Aerius Comprimé 5mg",            "Desloratadine",  "R06AX27", "Tablet",       False),
        ("Mopral Gélule 20mg",             "Omeprazole",     "A02BC01", "Capsule",      False),
        ("Imodium Gélule 2mg",             "Loperamide",     "A07DA03", "Capsule",      False),
        ("Ratiopharm Ibuprofen 400mg",     "Ibuprofen",      "M01AE01", "Tablet",       False),
        ("Bisoprolol Ratiopharm 5mg",      "Bisoprolol",     "C07AB07", "Tablet",       True),
        ("Metformin Ratiopharm 1000mg",    "Metformin",      "A10BA02", "Tablet",       True),
        ("Atorvastatin Ratiopharm 20mg",   "Atorvastatin",   "C10AA05", "Tablet",       True),
    ]
    for name, inn, atc, form, rx in lu_brands:
        rows.append({"name": name, "generic": inn, "atc": atc,
                     "pharmaceutical_form": form, "rx": "Rx" if rx else "", "status": "authorised"})

    print(f"    LU merknamen: {len(rows)} records")
    return rows


# ================================================================
# MAIN
# ================================================================
FETCHERS = {
    "CZ": fetch_cz,
    "SK": fetch_sk,
    "HR": fetch_hr,
    "SI": fetch_si,
    "HU": fetch_hu,
    "RO": fetch_ro,
    "GR": fetch_gr,
    "LU": fetch_lu,
}

if COUNTRY not in FETCHERS:
    print(f"Onbekend land: {COUNTRY}. Kies uit: {', '.join(FETCHERS.keys())}")
    sys.exit(1)

print(f"\n🌍 {COUNTRY} — medicijnen ophalen...")

# Stap 1: EMA-basis (geldt voor alle EU-landen)
all_rows = fetch_ema()

# Stap 2: landsspecifieke bron
country_rows = FETCHERS[COUNTRY]()
all_rows.extend(country_rows)

# Dedupliceer op naam
seen = set()
unique = []
for r in all_rows:
    key = r["name"].strip().lower()
    if key and key not in seen:
        seen.add(key)
        unique.append(r)

print(f"\n  📊 Totaal unieke records: {len(unique)}")

if not unique:
    print(f"  ⚠️  Geen data voor {COUNTRY} — controleer URLs en probeer opnieuw")
    sys.exit(1)

write_csv(unique)
print(f"✅ {COUNTRY} klaar: {len(unique)} medicijnen → {OUTPUT}")
