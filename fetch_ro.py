#!/usr/bin/env python3
"""
Romania (RO) — Medicines fetcher
Source: ANMDMR (Agenția Națională a Medicamentului și a Dispozitivelor Medicale din România)
URL: https://anm.ro/
Data: Downloadable Excel/CSV from medicines register

NOTE: Verify exact URL at https://anm.ro/medicamente/medicamente-autorizate/
      The download link changes periodically.
"""

import requests
import json
import os
import sys

ANMDMR_URL = "https://anm.ro/wp-content/uploads/lista_medicamente.xlsx"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_ro.js")

FORM_MAP = {
    "comprimat":                "Tablet",
    "capsulă":                  "Capsule",
    "soluție injectabilă":      "Solution for injection",
    "sirop":                    "Syrup",
    "cremă":                    "Cream",
    "unguent":                  "Ointment",
    "picături oftalmice":       "Eye drops",
    "spray nazal":              "Nasal spray",
    "pulbere":                  "Powder",
    "soluție orală":            "Oral solution",
    "suspensie":                "Suspension",
    "gel":                      "Gel",
    "plasture":                 "Patch",
    "pulbere de inhalat":       "Inhaler",
}

CATEGORY_KEYWORDS = {
    "Pain & Fever":           ["paracetamol", "ibuprofen", "diclofenac", "tramadol"],
    "Antibiotics":            ["amoxicilină", "azitromicină", "ciprofloxacin"],
    "Heart & Blood Pressure": ["amlodipină", "bisoprolol", "ramipril", "losartan"],
    "Diabetes":               ["metformin", "insulină", "glimepirid"],
    "Stomach & Intestine":    ["omeprazol", "pantoprazol", "loperamid"],
    "Cholesterol":            ["atorvastatină", "simvastatină", "rosuvastatină"],
    "Allergy":                ["cetirizin", "loratadin", "desloratadin"],
    "Cough & Cold":           ["xilometazolină", "pseudoefedrină"],
    "Lungs & Asthma":         ["salbutamol", "budesonid", "formoterol"],
    "Antidepressants":        ["sertralină", "escitalopram", "venlafaxină"],
    "Sleep & Sedation":       ["zolpidem", "zopiclonă", "diazepam"],
    "Skin & Wounds":          ["hidrocortizon", "betametazonă", "clotrimazol"],
    "Thyroid":                ["levotiroxin", "carbimazol"],
    "Vitamins & Supplements": ["vitamina d", "acid folic", "fier"],
}


def map_category(generic_name: str) -> str:
    name_lower = generic_name.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in kws):
            return cat
    return "Other"


def map_form(form_ro: str) -> str:
    form_lower = form_ro.lower()
    for ro_key, en_val in FORM_MAP.items():
        if ro_key in form_lower:
            return en_val
    return form_ro.capitalize()


def fetch_anmdmr_data():
    try:
        import openpyxl
        import io

        print("Fetching ANMDMR (RO) data...")
        resp = requests.get(ANMDMR_URL, timeout=60)
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
            # Adjust column names after inspecting the actual downloaded file
            name    = str(row_dict.get("Denumire comerciala", "") or "").strip()
            generic = str(row_dict.get("DCI", "") or "").strip()  # DCI = INN in Romanian
            form_ro = str(row_dict.get("Forma farmaceutica", "") or "").strip()

            if not name:
                continue

            medicines.append({
                "name":     name,
                "generic":  generic or name,
                "category": map_category(generic),
                "form":     map_form(form_ro),
                "rx":       True,
            })

        return medicines

    except Exception as e:
        print(f"Error fetching ANMDMR data: {e}", file=sys.stderr)
        return []


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Romania (RO) — medicines\n// Source: ANMDMR (anm.ro)\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_anmdmr_data()
    if data:
        write_output(data)
    else:
        print("No data — check ANMDMR_URL and column names.", file=sys.stderr)
        sys.exit(1)
