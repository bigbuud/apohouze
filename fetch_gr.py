#!/usr/bin/env python3
"""
Greece (GR) — Medicines fetcher
Source: EOF (Εθνικός Οργανισμός Φαρμάκων — National Organisation for Medicines)
URL: https://www.eof.gr/
Data: Downloadable medicines register

NOTE: EOF provides search at https://www.eof.gr/web/guest/eofapproved2
      The download format is typically Excel. Verify the URL below.
"""

import requests
import json
import os
import sys

EOF_URL = "https://www.eof.gr/web/guest/eofapproved2/-/asset_publisher/vKT1/content/download"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_gr.js")

FORM_MAP = {
    "δισκίο":                   "Tablet",
    "κάψουλα":                  "Capsule",
    "ενέσιμο διάλυμα":          "Solution for injection",
    "σιρόπι":                   "Syrup",
    "κρέμα":                    "Cream",
    "αλοιφή":                   "Ointment",
    "οφθαλμικές σταγόνες":      "Eye drops",
    "ρινικό σπρέι":              "Nasal spray",
    "κόνις":                    "Powder",
    "πόσιμο διάλυμα":           "Oral solution",
    "εναιώρημα":                "Suspension",
    "γέλη":                     "Gel",
    "επίθεμα":                  "Patch",
    "κόνις εισπνοής":           "Inhaler",
}

CATEGORY_KEYWORDS = {
    "Pain & Fever":           ["παρακεταμόλη", "ιβουπροφαίνη", "δικλοφενάκη", "τραμαδόλη"],
    "Antibiotics":            ["αμοξικιλλίνη", "αζιθρομυκίνη", "σιπροφλοξασίνη"],
    "Heart & Blood Pressure": ["αμλοδιπίνη", "βισοπρολόλη", "ραμιπρίλη", "λοσαρτάνη"],
    "Diabetes":               ["μετφορμίνη", "ινσουλίνη", "γλιμεπιρίδη"],
    "Stomach & Intestine":    ["ομεπραζόλη", "παντοπραζόλη", "λοπεραμίδη"],
    "Cholesterol":            ["ατορβαστατίνη", "σιμβαστατίνη", "ροσουβαστατίνη"],
    "Allergy":                ["σετιριζίνη", "λοραταδίνη", "δεσλοραταδίνη"],
    "Cough & Cold":           ["ξυλομεταζολίνη", "ψευδοεφεδρίνη"],
    "Lungs & Asthma":         ["σαλβουταμόλη", "βουδεσονίδη", "φορμοτερόλη"],
    "Antidepressants":        ["σερτραλίνη", "εσκιταλοπράμη", "βενλαφαξίνη"],
    "Sleep & Sedation":       ["ζολπιδέμη", "ζοπικλόνη", "διαζεπάμη"],
    "Skin & Wounds":          ["υδροκορτιζόνη", "βηταμεθαζόνη", "κλοτριμαζόλη"],
    "Thyroid":                ["λεβοθυροξίνη", "καρβιμαζόλη"],
    "Vitamins & Supplements": ["βιταμίνη d", "φολικό οξύ", "σίδηρος"],
    # Also include Latin names as fallback
    "Pain & Fever2":          ["paracetamol", "ibuprofen", "diclofenac"],
    "Antibiotics2":           ["amoxicillin", "azithromycin", "ciprofloxacin"],
}


def map_category(generic_name: str) -> str:
    name_lower = generic_name.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        cat_clean = cat.rstrip("0123456789")
        if any(kw in name_lower for kw in kws):
            return cat_clean
    return "Other"


def map_form(form_gr: str) -> str:
    form_lower = form_gr.lower()
    for gr_key, en_val in FORM_MAP.items():
        if gr_key in form_lower:
            return en_val
    return form_gr.capitalize()


def fetch_eof_data():
    """
    EOF may return Excel or may require a POST with search params.
    This is a simplified GET approach — adjust if the site uses POST/AJAX.
    """
    try:
        import openpyxl
        import io

        print("Fetching EOF (GR) data...")
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; apoHouze/1.0)",
            "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        resp = requests.get(EOF_URL, headers=headers, timeout=60)
        resp.raise_for_status()

        wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True)
        ws = wb.active

        medicines = []
        headers_row = None

        for row in ws.iter_rows(values_only=True):
            if headers_row is None:
                headers_row = [str(c).strip() if c else "" for c in row]
                continue
            row_dict = dict(zip(headers_row, row))
            # Adjust column names after inspecting the actual file
            name    = str(row_dict.get("Εμπορική Ονομασία", "") or "").strip()
            generic = str(row_dict.get("INN", "") or row_dict.get("Δραστική Ουσία", "") or "").strip()
            form_gr = str(row_dict.get("Φαρμακοτεχνική Μορφή", "") or "").strip()

            if not name:
                continue

            medicines.append({
                "name":     name,
                "generic":  generic or name,
                "category": map_category(generic),
                "form":     map_form(form_gr),
                "rx":       True,
            })

        return medicines

    except Exception as e:
        print(f"Error fetching EOF data: {e}", file=sys.stderr)
        return []


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Greece (GR) — medicines\n// Source: EOF (eof.gr)\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_eof_data()
    if data:
        write_output(data)
    else:
        print("No data — check EOF_URL and column names.", file=sys.stderr)
        sys.exit(1)
