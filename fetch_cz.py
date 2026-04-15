#!/usr/bin/env python3
"""
Czech Republic (CZ) — Medicines fetcher
Source: SÚKL (Státní ústav pro kontrolu léčiv)
URL: https://opendata.sukl.cz/
Data format: XML/CSV download

NOTE: Verify the exact download URL at https://opendata.sukl.cz/
      The endpoint below is based on known SÚKL open data structure.
"""

import requests
import json
import os
import sys

# Known SÚKL open data endpoint — verify at opendata.sukl.cz
SUKL_URL = "https://opendata.sukl.cz/soubory/DLP.zip"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_cz.js")

FORM_MAP = {
    "tableta": "Tablet",
    "tobolka": "Capsule",
    "injekční roztok": "Solution for injection",
    "sirup": "Syrup",
    "krém": "Cream",
    "mast": "Ointment",
    "oční kapky": "Eye drops",
    "nosní sprej": "Nasal spray",
    "prášek": "Powder",
    "perorální roztok": "Oral solution",
    "suspenze": "Suspension",
    "gel": "Gel",
    "náplast": "Patch",
    "inhalační prášek": "Inhaler",
}

CATEGORY_KEYWORDS = {
    "Pain & Fever":         ["paracetamol", "ibuprofen", "diklofenak", "tramadol", "morfin"],
    "Antibiotics":          ["amoxicilin", "azithromycin", "ciprofloxacin", "doxycyklin"],
    "Heart & Blood Pressure": ["amlodipin", "bisoprolol", "ramipril", "losartan", "furosemid"],
    "Diabetes":             ["metformin", "insulin", "glimepirid", "sitagliptin"],
    "Stomach & Intestine":  ["omeprazol", "pantoprazol", "loperamid", "metoklopramid"],
    "Cholesterol":          ["atorvastatin", "simvastatin", "rosuvastatin"],
    "Allergy":              ["cetirizin", "loratadin", "desloratadin", "fexofenadin"],
    "Cough & Cold":         ["xylometazolin", "pseudoefedrin", "dextromethorfan"],
    "Lungs & Asthma":       ["salbutamol", "budesonid", "formoterol", "tiotropium"],
    "Antidepressants":      ["sertralin", "escitalopram", "venlafaxin", "mirtazapin"],
    "Sleep & Sedation":     ["zolpidem", "zopiklon", "diazepam", "lorazepam"],
    "Skin & Wounds":        ["hydrokortison", "betametazon", "klotrimazol", "aciklovir"],
    "Thyroid":              ["levothyroxin", "karbimazol"],
    "Joints & Muscles":     ["methotrexat", "hydroxychlorochin", "alopurinol"],
    "Vitamins & Supplements": ["vitamin d", "kyselina listová", "železo", "hořčík"],
}


def map_category(generic_name: str) -> str:
    name_lower = generic_name.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return category
    return "Other"


def map_form(form_cz: str) -> str:
    form_lower = form_cz.lower()
    for cz_key, en_val in FORM_MAP.items():
        if cz_key in form_lower:
            return en_val
    return form_cz.capitalize()


def fetch_sukl_data():
    """
    SÚKL provides open data as a ZIP with CSV files.
    Columns include: KOD, NAZEV, INN, FORMA, SILA, DRZITEL, STAV
    """
    try:
        import zipfile
        import io
        import csv

        print("Fetching SÚKL data...")
        resp = requests.get(SUKL_URL, timeout=60)
        resp.raise_for_status()

        medicines = []
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            # Find the main medicines CSV (name may vary — check the ZIP)
            csv_files = [f for f in z.namelist() if f.endswith(".csv")]
            if not csv_files:
                raise ValueError("No CSV found in SÚKL ZIP")

            for csv_name in csv_files:
                with z.open(csv_name) as f:
                    reader = csv.DictReader(
                        (line.decode("utf-8-sig") for line in f),
                        delimiter=";"
                    )
                    for row in reader:
                        name    = row.get("NAZEV", "").strip()
                        generic = row.get("INN", "").strip()
                        form_cz = row.get("FORMA", "").strip()
                        status  = row.get("STAV", "").strip()

                        # Only active/registered medicines
                        if status not in ("R", "registrován", ""):
                            continue
                        if not name:
                            continue

                        medicines.append({
                            "name":     name,
                            "generic":  generic or name,
                            "category": map_category(generic),
                            "form":     map_form(form_cz),
                            "rx":       True,   # Default; SÚKL data includes OTC flag — update if available
                        })

        return medicines

    except Exception as e:
        print(f"Error fetching SÚKL data: {e}", file=sys.stderr)
        return []


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Czech Republic (CZ) — medicines\n"
    js += "// Source: SÚKL open data (opendata.sukl.cz)\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_sukl_data()
    if data:
        write_output(data)
    else:
        print("No data fetched — check SUKL_URL and data format.", file=sys.stderr)
        sys.exit(1)
