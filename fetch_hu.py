#!/usr/bin/env python3
"""
Hungary (HU) — Medicines fetcher
Source: OGYÉI (Nemzeti Gyógyszerészeti és Élelmezés-egészségügyi Intézet)
URL: https://www.ogyei.gov.hu/
Data: Downloadable Excel/CSV

NOTE: Verify at https://www.ogyei.gov.hu/gyogyszeradatbazis
      Column names may differ — inspect the first row of the downloaded file.
"""

import requests
import json
import os
import sys

OGYEI_URL = "https://www.ogyei.gov.hu/gyogyszeradatbazis/download/engedely.xlsx"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_hu.js")

FORM_MAP = {
    "tabletta":         "Tablet",
    "kapszula":         "Capsule",
    "injekciós oldat":  "Solution for injection",
    "szirup":           "Syrup",
    "krém":             "Cream",
    "kenőcs":           "Ointment",
    "szemcsepp":        "Eye drops",
    "orrspray":         "Nasal spray",
    "por":              "Powder",
    "orális oldat":     "Oral solution",
    "szuszpenzió":      "Suspension",
    "gél":              "Gel",
    "tapasz":           "Patch",
    "inhalációs por":   "Inhaler",
}

CATEGORY_KEYWORDS = {
    "Pain & Fever":           ["paracetamol", "ibuprofen", "diklofenák", "tramadol"],
    "Antibiotics":            ["amoxicilin", "azitromicin", "ciprofloxacin"],
    "Heart & Blood Pressure": ["amlodipin", "bisoprolol", "ramipril", "losartan"],
    "Diabetes":               ["metformin", "inzulin", "glimepirid"],
    "Stomach & Intestine":    ["omeprazol", "pantoprazol", "loperamid"],
    "Cholesterol":            ["atorvastatin", "simvastatin", "rosuvastatin"],
    "Allergy":                ["cetirizin", "loratadin", "desloratadin"],
    "Cough & Cold":           ["xilometazolin", "pszeudoefedrin"],
    "Lungs & Asthma":         ["szalbutamol", "budezonid", "formoterol"],
    "Antidepressants":        ["szertralín", "eszcitolapram", "venlafaxin"],
    "Sleep & Sedation":       ["zolpidem", "zopiklon", "diazepam"],
    "Skin & Wounds":          ["hidrokortizon", "betametazon", "klotrimazol"],
    "Thyroid":                ["levothyroxin", "karbimazol"],
    "Vitamins & Supplements": ["d-vitamin", "folsav", "vas"],
}


def map_category(generic_name: str) -> str:
    name_lower = generic_name.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in kws):
            return cat
    return "Other"


def map_form(form_hu: str) -> str:
    form_lower = form_hu.lower()
    for hu_key, en_val in FORM_MAP.items():
        if hu_key in form_lower:
            return en_val
    return form_hu.capitalize()


def fetch_ogyei_data():
    try:
        import openpyxl
        import io

        print("Fetching OGYÉI (HU) data...")
        resp = requests.get(OGYEI_URL, timeout=60)
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
            # Adjust column names after inspecting the actual file
            name    = str(row_dict.get("Készítmény neve", "") or "").strip()
            generic = str(row_dict.get("INN", "") or "").strip()
            form_hu = str(row_dict.get("Gyógyszerforma", "") or "").strip()

            if not name:
                continue

            medicines.append({
                "name":     name,
                "generic":  generic or name,
                "category": map_category(generic),
                "form":     map_form(form_hu),
                "rx":       True,
            })

        return medicines

    except Exception as e:
        print(f"Error fetching OGYÉI data: {e}", file=sys.stderr)
        return []


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Hungary (HU) — medicines\n// Source: OGYÉI (ogyei.gov.hu)\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_ogyei_data()
    if data:
        write_output(data)
    else:
        print("No data — check OGYEI_URL and column names.", file=sys.stderr)
        sys.exit(1)
