#!/usr/bin/env python3
"""
fetch_hr.py — Croatia (HR)
Source: HALMED (Agencija za lijekove i medicinske proizvode)
URL: https://www.halmed.hr/
Data: Downloadable register of authorized medicines

NOTE: Verify exact URL at https://www.halmed.hr/lijekovi/baza-lijekova/
"""

import requests
import json
import os
import sys

HALMED_URL = "https://www.halmed.hr/lijekovi/baza-lijekova/download/"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_hr.js")

FORM_MAP = {
    "tableta":              "Tablet",
    "kapsula":              "Capsule",
    "otopina za injekciju": "Solution for injection",
    "sirup":                "Syrup",
    "krema":                "Cream",
    "mast":                 "Ointment",
    "kapi za oči":          "Eye drops",
    "nosni sprej":          "Nasal spray",
    "prašak":               "Powder",
    "peroralna otopina":    "Oral solution",
    "suspenzija":           "Suspension",
    "gel":                  "Gel",
    "flaster":              "Patch",
    "inhaler":              "Inhaler",
}

CATEGORY_KEYWORDS = {
    "Pain & Fever":           ["paracetamol", "ibuprofen", "diklofenak", "tramadol"],
    "Antibiotics":            ["amoksicilin", "azitromicin", "ciprofloksacin", "doksiciklin"],
    "Heart & Blood Pressure": ["amlodipin", "bisoprolol", "ramipril", "losartan"],
    "Diabetes":               ["metformin", "inzulin", "glimepirid", "sitagliptin"],
    "Stomach & Intestine":    ["omeprazol", "pantoprazol", "loperamid"],
    "Cholesterol":            ["atorvastatin", "simvastatin", "rosuvastatin"],
    "Allergy":                ["cetirizin", "loratadin", "desloratadin"],
    "Cough & Cold":           ["ksilometazolin", "pseudoefedrin"],
    "Lungs & Asthma":         ["salbutamol", "budezonid", "formoterol"],
    "Antidepressants":        ["sertralin", "escitalopram", "venlafaksin"],
    "Sleep & Sedation":       ["zolpidem", "zopiklon", "diazepam"],
    "Skin & Wounds":          ["hidrokortizon", "betametazon", "klotrimazol"],
    "Thyroid":                ["levotiroksin", "karbimazol"],
    "Vitamins & Supplements": ["vitamin d", "folna kiselina", "željezo"],
}


def map_category(generic_name: str) -> str:
    name_lower = generic_name.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in kws):
            return cat
    return "Other"


def map_form(form_hr: str) -> str:
    form_lower = form_hr.lower()
    for hr_key, en_val in FORM_MAP.items():
        if hr_key in form_lower:
            return en_val
    return form_hr.capitalize()


def fetch_halmed_data():
    """
    HALMED provides a downloadable Excel/CSV of authorised medicines.
    Columns: Naziv lijeka, INN, Farmaceutski oblik, Status
    """
    try:
        import openpyxl
        import io

        print("Fetching HALMED (HR) data...")
        resp = requests.get(HALMED_URL, timeout=60)
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
            name    = str(row_dict.get("Naziv lijeka", "") or "").strip()
            generic = str(row_dict.get("INN", "") or "").strip()
            form_hr = str(row_dict.get("Farmaceutski oblik", "") or "").strip()

            if not name:
                continue

            medicines.append({
                "name":     name,
                "generic":  generic or name,
                "category": map_category(generic),
                "form":     map_form(form_hr),
                "rx":       True,
            })

        return medicines

    except Exception as e:
        print(f"Error fetching HALMED data: {e}", file=sys.stderr)
        return []


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Croatia (HR) — medicines\n// Source: HALMED (halmed.hr)\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_halmed_data()
    if data:
        write_output(data)
    else:
        print("No data — check HALMED_URL.", file=sys.stderr)
        sys.exit(1)
