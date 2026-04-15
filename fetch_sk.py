#!/usr/bin/env python3
"""
Slovakia (SK) — Medicines fetcher
Source: ŠÚKL (Štátny ústav pre kontrolu liečiv)
URL: https://www.sukl.sk/
Data format: downloadable Excel/CSV

NOTE: ŠÚKL provides downloadable medicine lists.
      Verify exact URL at https://www.sukl.sk/registrovane-lieky
"""

import requests
import json
import os
import sys

SUKL_SK_URL = "https://www.sukl.sk/buxus/docs/lieky/register_liekov.xlsx"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_sk.js")

FORM_MAP = {
    "tableta":          "Tablet",
    "kapsula":          "Capsule",
    "injekčný roztok":  "Solution for injection",
    "sirup":            "Syrup",
    "krém":             "Cream",
    "masť":             "Ointment",
    "očné kvapky":      "Eye drops",
    "nosová aerodisperzia": "Nasal spray",
    "prášok":           "Powder",
    "perorálny roztok": "Oral solution",
    "suspenzia":        "Suspension",
    "gél":              "Gel",
    "náplasť":          "Patch",
    "inhalačný prášok": "Inhaler",
}

CATEGORY_KEYWORDS = {
    "Pain & Fever":         ["paracetamol", "ibuprofén", "diklofenak", "tramadol"],
    "Antibiotics":          ["amoxicilín", "azitromycín", "ciprofloxacín", "doxycyklín"],
    "Heart & Blood Pressure": ["amlodipín", "bisoprolol", "ramipril", "losartan"],
    "Diabetes":             ["metformín", "inzulín", "glimepirid", "sitagliptín"],
    "Stomach & Intestine":  ["omeprazol", "pantoprazol", "loperamid"],
    "Cholesterol":          ["atorvastatín", "simvastatín", "rosuvastatín"],
    "Allergy":              ["cetirízín", "loratadín", "desloratadín"],
    "Cough & Cold":         ["xylometazolín", "pseudoefedrín"],
    "Lungs & Asthma":       ["salbutamol", "budezonid", "formoterol", "tiotropium"],
    "Antidepressants":      ["sertralín", "escitalopram", "venlafaxín"],
    "Sleep & Sedation":     ["zolpidem", "zopiklón", "diazepam"],
    "Skin & Wounds":        ["hydrokortizón", "betametazón", "klotrimazol"],
    "Thyroid":              ["levotyroxín", "karbimazol"],
    "Joints & Muscles":     ["metotrexát", "hydroxy chlorochín", "alopurinol"],
    "Vitamins & Supplements": ["vitamín d", "kyselina listová", "železo"],
}


def map_category(generic_name: str) -> str:
    name_lower = generic_name.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in kws):
            return cat
    return "Other"


def map_form(form_sk: str) -> str:
    form_lower = form_sk.lower()
    for sk_key, en_val in FORM_MAP.items():
        if sk_key in form_lower:
            return en_val
    return form_sk.capitalize()


def fetch_sukl_sk_data():
    """
    ŠÚKL provides an Excel register of licensed medicines.
    Expected columns: Názov lieku, INN, Lieková forma, Stav registrácie
    """
    try:
        import openpyxl
        import io

        print("Fetching ŠÚKL SK data...")
        resp = requests.get(SUKL_SK_URL, timeout=60)
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
            name    = str(row_dict.get("Názov lieku", "") or "").strip()
            generic = str(row_dict.get("INN", "") or "").strip()
            form_sk = str(row_dict.get("Lieková forma", "") or "").strip()
            status  = str(row_dict.get("Stav", "") or "").strip().lower()

            if "zrušen" in status or "expired" in status:
                continue
            if not name:
                continue

            medicines.append({
                "name":     name,
                "generic":  generic or name,
                "category": map_category(generic),
                "form":     map_form(form_sk),
                "rx":       True,
            })

        return medicines

    except Exception as e:
        print(f"Error fetching ŠÚKL SK data: {e}", file=sys.stderr)
        return []


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Slovakia (SK) — medicines\n"
    js += "// Source: ŠÚKL (sukl.sk)\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_sukl_sk_data()
    if data:
        write_output(data)
    else:
        print("No data — check SUKL_SK_URL and column names.", file=sys.stderr)
        sys.exit(1)
