#!/usr/bin/env python3
"""
apoHouze — Duitsland Medicijnen Fetcher
=======================================
Haalt geneesmiddelendata op voor Duitsland uit meerdere officiële bronnen:

1. EMA (European Medicines Agency) — gecentraliseerde EU-vergunningen
   https://www.ema.europa.eu/en/documents/report/medicines-output_en.xlsx

2. BfArM Arzneimittel-Informationssystem (AMIS) — nationale vergunningen
   https://www.bfarm.de/EN/Medicines/Marketing-Authorisation/pharmaceutical-register/downloads/

Output: data/_tmp/de_medicines.csv
  Kolommen: Name,INN,ATC,PharmaceuticalForm,RxStatus,Country

Gebruik: python3 fetch_de_medicines.py [--debug]
"""

import sys
import os
import re
import time
import urllib.request
import urllib.error

DEBUG = "--debug" in sys.argv

# ================================================================
# CONFIG
# ================================================================
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "data", "_tmp", "de_medicines.csv")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 apoHouze-updater/5.0 (https://github.com/bigbuud/apohouze)",
    "Accept": "*/*",
}

# ATC → categorie mapping (zelfde als update.js)
ATC_MAP = {
    "A02": "Stomach & Intestine", "A03": "Stomach & Intestine", "A04": "Stomach & Intestine",
    "A05": "Stomach & Intestine", "A06": "Stomach & Intestine", "A07": "Stomach & Intestine",
    "A08": "Stomach & Intestine", "A09": "Stomach & Intestine", "A10": "Diabetes",
    "A11": "Vitamins & Supplements", "A12": "Vitamins & Supplements", "A13": "Vitamins & Supplements",
    "A16": "Stomach & Intestine",
    "B01": "Anticoagulants", "B02": "Heart & Blood Pressure", "B03": "Vitamins & Supplements",
    "B05": "Heart & Blood Pressure", "B06": "Heart & Blood Pressure",
    "C01": "Heart & Blood Pressure", "C02": "Heart & Blood Pressure", "C03": "Heart & Blood Pressure",
    "C04": "Heart & Blood Pressure", "C05": "Heart & Blood Pressure", "C07": "Heart & Blood Pressure",
    "C08": "Heart & Blood Pressure", "C09": "Heart & Blood Pressure", "C10": "Cholesterol",
    "D01": "Antifungals", "D02": "Skin & Wounds", "D03": "Skin & Wounds", "D04": "Skin & Wounds",
    "D05": "Skin & Wounds", "D06": "Antibiotics", "D07": "Corticosteroids", "D08": "Skin & Wounds",
    "D09": "Skin & Wounds", "D10": "Skin & Wounds", "D11": "Skin & Wounds",
    "G01": "Women's Health", "G02": "Women's Health", "G03": "Women's Health", "G04": "Urology",
    "H01": "Thyroid", "H02": "Corticosteroids", "H03": "Thyroid", "H04": "Diabetes",
    "H05": "Vitamins & Supplements",
    "J01": "Antibiotics", "J02": "Antifungals", "J04": "Antibiotics", "J05": "Antivirals",
    "J06": "Antivirals", "J07": "Antivirals",
    "L01": "Oncology", "L02": "Oncology", "L03": "Oncology", "L04": "Corticosteroids",
    "M01": "Pain & Fever", "M02": "Joints & Muscles", "M03": "Joints & Muscles",
    "M04": "Joints & Muscles", "M05": "Joints & Muscles", "M09": "Joints & Muscles",
    "N01": "Pain & Fever", "N02": "Pain & Fever", "N03": "Neurology", "N04": "Neurology",
    "N05": "Sleep & Sedation", "N06": "Antidepressants", "N07": "Nervous System",
    "P01": "Antiparasitics", "P02": "Antiparasitics", "P03": "Antiparasitics",
    "R01": "Cough & Cold", "R02": "Cough & Cold", "R03": "Lungs & Asthma",
    "R04": "Cough & Cold", "R05": "Cough & Cold", "R06": "Allergy", "R07": "Lungs & Asthma",
    "S01": "Eye & Ear", "S02": "Eye & Ear", "S03": "Eye & Ear",
    "V03": "First Aid", "V06": "Vitamins & Supplements", "V07": "First Aid", "V08": "First Aid",
}

def atc_to_category(atc):
    if not atc:
        return None
    return ATC_MAP.get(atc.strip()[:3].upper())

def download(url, dest, timeout=120):
    """Download bestand via urllib met retry."""
    print(f"  📥 {url}")
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r, open(dest, "wb") as f:
                f.write(r.read())
            size = os.path.getsize(dest)
            print(f"  ✅ {size // 1024} KB gedownload")
            return size
        except Exception as e:
            print(f"  ⚠️  Poging {attempt+1}/3 mislukt: {e}")
            if attempt < 2:
                time.sleep(3)
    raise RuntimeError(f"Download mislukt na 3 pogingen: {url}")

def read_xlsx(path):
    """Lees xlsx met openpyxl of pandas als fallback."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [str(h).strip() if h else "" for h in rows[0]]
        result = []
        for row in rows[1:]:
            result.append({headers[i]: (str(row[i]).strip() if row[i] is not None else "") for i in range(len(headers))})
        wb.close()
        return result
    except ImportError:
        pass
    try:
        import pandas as pd
        df = pd.read_excel(path)
        return df.to_dict("records")
    except ImportError:
        raise RuntimeError("openpyxl of pandas vereist: pip install openpyxl pandas")

def read_csv(path, sep=None):
    """Lees CSV en detecteer separator automatisch."""
    import csv
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)
        if sep is None:
            sniffer = csv.Sniffer()
            try:
                sep = sniffer.sniff(sample, delimiters=",;\t|").delimiter
            except Exception:
                sep = ","
        reader = csv.DictReader(f, delimiter=sep)
        return list(reader)

# ================================================================
# BRON 1: EMA (gecentraliseerde EU-vergunningen)
# Bevat grote merknamen die ook in Duitsland verkrijgbaar zijn.
# ================================================================
def load_ema():
    """
    EMA Medicines output — Excel bestand met alle centraal vergunde middelen.
    URL: https://www.ema.europa.eu/en/documents/report/medicines-output_en.xlsx
    
    Relevante kolommen:
      - Medicine name          → merknaam
      - Active substance       → generieke naam / INN
      - ATC code               → ATC (voor categorie)
      - Authorisation status   → status (filter: Authorised)
      - Pharmaceutical form    → vorm
    """
    url = "https://www.ema.europa.eu/en/documents/report/medicines-output_en.xlsx"
    dest = os.path.join(os.path.dirname(OUTPUT_FILE), "ema_raw.xlsx")
    
    try:
        download(url, dest)
    except Exception as e:
        print(f"  ❌ EMA download mislukt: {e}")
        return []
    
    try:
        rows = read_xlsx(dest)
    except Exception as e:
        print(f"  ❌ EMA lezen mislukt: {e}")
        return []
    
    if DEBUG:
        if rows:
            print(f"  🔍 EMA kolommen: {list(rows[0].keys())[:10]}")
    
    results = []
    skipped = 0
    for row in rows:
        # Flexibele kolomnamen zoeken
        name = next((row[k] for k in row if "medicine name" in k.lower() or k.lower() == "name"), "").strip()
        inn  = next((row[k] for k in row if "active substance" in k.lower() or "inn" in k.lower()), "").strip()
        atc  = next((row[k] for k in row if k.lower().startswith("atc")), "").strip()
        form = next((row[k] for k in row if "pharmaceutical form" in k.lower() or "form" in k.lower()), "").strip()
        status = next((row[k] for k in row if "authoris" in k.lower() or "status" in k.lower()), "").strip()
        
        if not name:
            skipped += 1
            continue
        # Filter: alleen goedgekeurde middelen
        if status and re.search(r"withdrawn|refused|suspended|expired", status, re.I):
            skipped += 1
            continue
        # Blacklist medische hulpmiddelen
        if re.search(r"\b(device|diagnostic|kit|test|imaging)\b", name, re.I):
            skipped += 1
            continue
        
        results.append({
            "Name": name,
            "INN": inn,
            "ATC": atc,
            "PharmaceuticalForm": form,
            "RxStatus": "Rx",  # EMA producten zijn altijd Rx
            "Country": "EU",
        })
    
    print(f"  📊 EMA: {len(results)} geladen, {skipped} overgeslagen")
    return results

# ================================================================
# BRON 2: BfArM AMIS — nationale Duitse vergunningen
# Het Bundesinstitut für Arzneimittel und Medizinprodukte publiceert
# een downloadbare lijst van nationaal vergunde geneesmiddelen.
# URL: https://www.bfarm.de/EN/Medicines/Marketing-Authorisation/pharmaceutical-register/
# 
# AMIS download: rechtstreekse CSV/XLSX via open data portal
# ================================================================
def load_bfarm():
    """
    BfArM AMIS — publieke medicijnenlijst (nationale vergunningen DE).
    
    BfArM biedt de AMIS-database aan als downloadbare CSV.
    Kolommen variëren; we zoeken flexibel naar naam/ATC/vorm.
    """
    # BfArM publiceert een CSV-export van het farmaceutisch register
    urls = [
        # Primaire bron: BfArM open data
        "https://www.bfarm.de/SharedDocs/Downloads/EN/Medicines/database_amis.csv;jsessionid=",
        "https://www.bfarm.de/SharedDocs/Downloads/DE/Arzneimittel/Zulassung/amis-download.csv",
        # Fallback: DIMDI (nu onder BfArM gevallen)
        "https://www.dimdi.de/dynamic/de/arzneimittel/datenbanken/downloads/amisdaten.csv",
    ]
    
    dest = os.path.join(os.path.dirname(OUTPUT_FILE), "bfarm_raw.csv")
    
    downloaded = False
    for url in urls:
        try:
            size = download(url, dest)
            if size > 5000:
                downloaded = True
                break
        except Exception as e:
            if DEBUG:
                print(f"  ⚠️  BfArM URL mislukt: {url[:60]}... — {e}")
            continue
    
    if not downloaded:
        print("  ⚠️  BfArM directe download niet beschikbaar — wordt overgeslagen")
        print("       (BfArM vereist soms browsernavigatie; EMA-data wordt als basis gebruikt)")
        return []
    
    try:
        rows = read_csv(dest)
    except Exception as e:
        print(f"  ❌ BfArM lezen mislukt: {e}")
        return []
    
    if DEBUG and rows:
        print(f"  🔍 BfArM kolommen: {list(rows[0].keys())[:10]}")
    
    results = []
    skipped = 0
    for row in rows:
        name = next((row[k] for k in row if re.search(r"(bezeichnung|handelsname|name|produkt)", k, re.I)), "").strip()
        inn  = next((row[k] for k in row if re.search(r"(wirkstoff|inn|substanz|aktiv)", k, re.I)), "").strip()
        atc  = next((row[k] for k in row if re.search(r"^atc", k, re.I)), "").strip()
        form = next((row[k] for k in row if re.search(r"(darreichung|form|arznei)", k, re.I)), "").strip()
        status = next((row[k] for k in row if re.search(r"(status|zulass|zulassung)", k, re.I)), "").strip()
        rx_raw = next((row[k] for k in row if re.search(r"(verschreibung|rezept|rx|abgabe)", k, re.I)), "").strip()
        
        if not name:
            skipped += 1
            continue
        if status and re.search(r"(ruhend|widerrufen|zurückgez|expired|withdrawn)", status, re.I):
            skipped += 1
            continue
        
        rx_str = "Rx" if re.search(r"(verschreibung|rezeptpflichtig|rpfl)", rx_raw, re.I) else "OTC"
        
        results.append({
            "Name": name,
            "INN": inn,
            "ATC": atc,
            "PharmaceuticalForm": form,
            "RxStatus": rx_str,
            "Country": "DE",
        })
    
    print(f"  📊 BfArM: {len(results)} geladen, {skipped} overgeslagen")
    return results

# ================================================================
# DEDUPLICATIE & FILTER
# ================================================================
def merge_and_filter(datasets):
    """Combineer datasets, dedupliceer op naam+INN, filter zonder ATC."""
    all_rows = []
    for ds in datasets:
        all_rows.extend(ds)
    
    print(f"\n  📦 Totaal voor dedup: {len(all_rows)} rijen")
    
    seen = set()
    filtered = []
    no_atc = 0
    
    for row in all_rows:
        name = row.get("Name", "").strip()
        inn  = row.get("INN", "").strip()
        atc  = row.get("ATC", "").strip()
        
        if not name:
            continue
        
        # ATC-categorie vereist
        if not atc_to_category(atc):
            no_atc += 1
            continue
        
        key = f"{name.lower()}|{inn.lower()}"
        if key in seen:
            continue
        seen.add(key)
        filtered.append(row)
    
    print(f"  🎯 Na dedup: {len(filtered)} unieke medicijnen ({no_atc} zonder ATC-categorie overgeslagen)")
    return filtered

# ================================================================
# OPSLAAN ALS CSV
# ================================================================
def save_csv(rows):
    import csv
    fieldnames = ["Name", "INN", "ATC", "PharmaceuticalForm", "RxStatus", "Country"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n✅ {len(rows)} medicijnen opgeslagen naar {OUTPUT_FILE}")

# ================================================================
# MAIN
# ================================================================
def main():
    print("🇩🇪 apoHouze — Duitsland Medicijnen Fetcher")
    print("=" * 50)
    
    datasets = []
    
    print("\n[1/2] EMA (gecentraliseerde EU-vergunningen)...")
    ema = load_ema()
    datasets.append(ema)
    
    print("\n[2/2] BfArM (nationale Duitse vergunningen)...")
    bfarm = load_bfarm()
    datasets.append(bfarm)
    
    merged = merge_and_filter(datasets)
    
    if not merged:
        print("\n❌ Geen data gevonden. Controleer de netwerktoegang.")
        sys.exit(1)
    
    save_csv(merged)

if __name__ == "__main__":
    main()
