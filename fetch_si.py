#!/usr/bin/env python3
"""
Slovenia (SI) — Medicines fetcher
Source: JAZMP (Javna agencija RS za zdravila in medicinske pripomočke)
URL: https://www.jazmp.si/
Data: Downloadable Excel from medicines register

NOTE: Verify exact download URL at https://www.jazmp.si/humana-zdravila/baza-podatkov-o-zdravilih/
"""

import requests
import json
import os
import sys

JAZMP_URL = "https://www.jazmp.si/fileadmin/datoteke/baza_zdravil/baza_zdravil.xlsx"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_si.js")

FORM_MAP = {
    "tableta":              "Tablet",
    "kapsula":              "Capsule",
    "raztopina za injiciranje": "Solution for injection",
    "sirup":                "Syrup",
    "krema":                "Cream",
    "mazilo":               "Ointment",
    "kapljice za oči":      "Eye drops",
    "nosno pršilo":         "Nasal spray",
    "prašek":               "Powder",
    "peroralna raztopina":  "Oral solution",
    "suspenzija":           "Suspension",
    "gel":                  "Gel",
    "obliž":                "Patch",
    "inhalacijski prašek":  "Inhaler",
}

CATEGORY_KEYWORDS = {
    "Pain & Fever":           ["paracetamol", "ibuprofen", "diklofenak", "tramadol"],
    "Antibiotics":            ["amoksicilin", "azitromicin", "ciprofloksacin"],
    "Heart & Blood Pressure": ["amlodipin", "bisoprolol", "ramipril", "losartan"],
    "Diabetes":               ["metformin", "inzulin", "glimepirid"],
    "Stomach & Intestine":    ["omeprazol", "pantoprazol", "loperamid"],
    "Cholesterol":            ["atorvastatin", "simvastatin", "rosuvastatin"],
    "Allergy":                ["cetirizin", "loratadin", "desloratadin"],
    "Cough & Cold":           ["ksilometazolin", "pseudoefedrin"],
    "Lungs & Asthma":         ["salbutamol", "budezonid", "formoterol"],
    "Antidepressants":        ["sertralin", "escitalopram", "venlafaksin"],
    "Sleep & Sedation":       ["zolpidem", "zopiklon", "diazepam"],
    "Skin & Wounds":          ["hidrokortizon", "betametazon", "klotrimazol"],
    "Thyroid":                ["levotiroksin", "karbimazol"],
    "Vitamins & Supplements": ["vitamin d", "folna kislina", "železo"],
}


def map_category(generic_name: str) -> str:
    name_lower = generic_name.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in kws):
            return cat
    return "Other"


def map_form(form_si: str) -> str:
    form_lower = form_si.lower()
    for si_key, en_val in FORM_MAP.items():
        if si_key in form_lower:
            return en_val
    return form_si.capitalize()


def fetch_jazmp_data():
    try:
        import openpyxl
        import io

        print("Fetching JAZMP (SI) data...")
        resp = requests.get(JAZMP_URL, timeout=60)
        resp.raise_for_status()

        wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True)
        ws = wb.active

        medicines = []
        headers = None

        for row in ws.iter_rows(values_only=True):
            if headers is None:
                headers = [str(c).strip() if c else "" for c in row]
                continue
            row_dict = dict(zip(headers, row))
            name    = str(row_dict.get("Ime zdravila", "") or "").strip()
            generic = str(row_dict.get("INN", "") or "").strip()
            form_si = str(row_dict.get("Farmacevtska oblika", "") or "").strip()

            if not name:
                continue

            medicines.append({
                "name":     name,
                "generic":  generic or name,
                "category": map_category(generic),
                "form":     map_form(form_si),
                "rx":       True,
            })

        return medicines

    except Exception as e:
        print(f"Error fetching JAZMP data: {e}", file=sys.stderr)
        return []


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Slovenia (SI) — medicines\n// Source: JAZMP (jazmp.si)\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_jazmp_data()
    if data:
        write_output(data)
    else:
        print("No data — check JAZMP_URL.", file=sys.stderr)
        sys.exit(1)
