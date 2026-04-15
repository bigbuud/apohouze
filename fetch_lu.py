#!/usr/bin/env python3
"""
Luxembourg (LU) — Medicines fetcher
Source: Ministère de la Santé Luxembourg
URL: https://sante.public.lu/fr/professionnels/medicaments.html

Luxembourg is a special case:
  - Most medicines are authorized via the Belgian SAM system or EMA
  - LU-specific national authorizations are few
  - The de facto medicine list overlaps heavily with BE + FR

Strategy:
  1. Try to fetch from the LU official source
  2. Fall back to deriving from the BE dataset with LU-relevant brands/names
  3. Luxembourg uses French AND German names depending on region

NOTE: If https://sante.public.lu provides a downloadable list, use that URL.
      Otherwise use the SAM CIVICS API (same as BE) with LU filter.
"""

import requests
import json
import os
import sys

# Option A: Direct download from LU Ministry of Health
LU_URL = "https://sante.public.lu/fr/professionnels/medicaments/telechargement.html"

# Option B: SAM API with country filter (same infrastructure as BE)
SAM_LU_URL = "https://api.ehealth.fgov.be/samws/v3/openapi/drug"

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/medicines_lu.js")

# Luxembourg-specific brand names (not in BE list but common in LU)
LU_SPECIFIC_BRANDS = [
    # OTC Pain & Fever
    {"name": "Doliprane Comprimé 500mg", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Doliprane Comprimé 1000mg", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Doliprane Suspension 2.4%", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Suspension", "rx": False},
    {"name": "Efferalgan Comprimé 500mg", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Effervescent tablet", "rx": False},
    {"name": "Efferalgan Comprimé 1000mg", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Effervescent tablet", "rx": False},
    {"name": "Dafalgan Comprimé 500mg", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Dafalgan Comprimé 1000mg", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Aspirine UPSA Comprimé 500mg", "generic": "Aspirin", "category": "Pain & Fever", "form": "Effervescent tablet", "rx": False},
    {"name": "Nurofen Comprimé 200mg", "generic": "Ibuprofen", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Nurofen Comprimé 400mg", "generic": "Ibuprofen", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Voltarène Emulgel 1%", "generic": "Diclofenac", "category": "Pain & Fever", "form": "Gel", "rx": False},
    {"name": "Voltarène Emulgel 2%", "generic": "Diclofenac", "category": "Pain & Fever", "form": "Gel", "rx": False},
    # Cough & Cold
    {"name": "Otrivine Spray nasal 0.1%", "generic": "Xylometazoline", "category": "Cough & Cold", "form": "Nasal spray", "rx": False},
    {"name": "Rhinathiol Sirop 2%", "generic": "Carbocisteine", "category": "Cough & Cold", "form": "Syrup", "rx": False},
    # Allergy
    {"name": "Zyrtec Comprimé 10mg", "generic": "Cetirizine", "category": "Allergy", "form": "Tablet", "rx": False},
    {"name": "Clarityne Comprimé 10mg", "generic": "Loratadine", "category": "Allergy", "form": "Tablet", "rx": False},
    {"name": "Aerius Comprimé 5mg", "generic": "Desloratadine", "category": "Allergy", "form": "Tablet", "rx": False},
    # Stomach
    {"name": "Mopral Gélule 20mg", "generic": "Omeprazole", "category": "Stomach & Intestine", "form": "Capsule", "rx": False},
    {"name": "Nexium Control Comprimé 20mg", "generic": "Esomeprazole", "category": "Stomach & Intestine", "form": "Tablet", "rx": False},
    {"name": "Imodium Gélule 2mg", "generic": "Loperamide", "category": "Stomach & Intestine", "form": "Capsule", "rx": False},
    # German-name brands (common in Luxembourgish-German context)
    {"name": "Ben-u-ron Tablette 500mg", "generic": "Paracetamol", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Thomapyrin Tablette", "generic": "Paracetamol / Aspirin / Caffeine", "category": "Pain & Fever", "form": "Tablet", "rx": False},
    {"name": "Ratiopharm Ibuprofen 400mg", "generic": "Ibuprofen", "category": "Pain & Fever", "form": "Tablet", "rx": False},
]


def fetch_from_lu_ministry():
    """
    Attempt to fetch directly from LU Ministry of Health.
    Returns [] if not available — will fall back to SAM.
    """
    try:
        resp = requests.get(LU_URL, timeout=30)
        if resp.status_code != 200:
            return []
        # TODO: parse the response if a download link or API is found
        return []
    except Exception:
        return []


def fetch_from_sam_be():
    """
    Belgium SAM API — LU often accepts BE-authorized products.
    Fetches a subset of frequently used medicines.
    """
    try:
        print("Fetching from SAM CIVICS (BE infrastructure for LU)...")
        resp = requests.get(
            SAM_LU_URL,
            params={"country": "LU", "status": "AUTHORIZED", "limit": 5000},
            timeout=60
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        medicines = []
        for item in data.get("results", []):
            name    = item.get("name", "")
            generic = item.get("inn", "") or item.get("generic", "")
            form    = item.get("pharmaceuticalForm", "")
            rx      = item.get("prescriptionRequired", True)
            if not name:
                continue
            medicines.append({
                "name":     name,
                "generic":  generic,
                "category": "Other",
                "form":     form,
                "rx":       rx,
            })
        return medicines
    except Exception:
        return []


def fetch_lu_data():
    # Try official LU source first
    medicines = fetch_from_lu_ministry()

    # Fall back to SAM/BE infrastructure
    if not medicines:
        medicines = fetch_from_sam_be()

    # Always add LU-specific brands
    medicines.extend(LU_SPECIFIC_BRANDS)

    # Deduplicate by name
    seen = set()
    unique = []
    for m in medicines:
        key = m["name"].lower()
        if key not in seen:
            seen.add(key)
            unique.append(m)

    return unique


def write_output(medicines: list):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    js = "// Luxembourg (LU) — medicines\n"
    js += "// Source: Ministère de la Santé LU + SAM CIVICS BE\n"
    js += "// Note: LU uses a mix of French and German brand names\n\n"
    js += f"const MEDICINES = {json.dumps(medicines, ensure_ascii=False, indent=2)};\n\n"
    js += "module.exports = { MEDICINES };\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"Written {len(medicines)} medicines to {OUTPUT_FILE}")


if __name__ == "__main__":
    data = fetch_lu_data()
    write_output(data)
