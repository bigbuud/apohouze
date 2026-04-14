#!/usr/bin/env python3
"""
apoHouze — Generiek EU Medicijnen Fetcher
==========================================
Werkt voor: AT, CH, DK, ES, FI, IE, NO, PL, PT, SE
(en als fallback voor DE, FR, IT)

Strategie per land:
  Alle landen: EMA JSON (centraal vergunde EU-middelen, ~1500 records, altijd bereikbaar)
  ES:  AEMPS CIMA REST API  — cima.aemps.es/cima/rest (officieel, open, geen auth)
  SE:  Läkemedelsverket CSV — lakemedelsverket.se open data
  NO:  Felleskatalogen/NoMA — legemiddelsok.no open data
  CH:  Swissmedic CSV       — swissmedic.ch open data
  AT:  BASG/ASP CSV         — basg.gv.at via data.gv.at
  DK:  DKMA produktresume   — medicinpriser.dk open data CSV
  FI:  Fimea open data      — fimea.fi / avoindata.fi
  PT:  INFARMED CSV         — infarmed.pt open data
  PL:  URPL XML/CSV         — rejestry.ezdrowie.gov.pl
  IE:  HPRA (CKAN API)      — hpra.ie / data.gov.ie

Gebruik: python3 fetch_eu_medicines.py <landcode>
  Voorbeeld: python3 fetch_eu_medicines.py es
"""

import sys, os, re, csv, time, subprocess, json, io, urllib.request
from urllib.parse import urlencode

if len(sys.argv) < 2:
    print(f"Gebruik: python3 {sys.argv[0]} <landcode>")
    sys.exit(1)

COUNTRY = sys.argv[1].upper()
REPO_ROOT = os.getcwd()
TMP_DIR   = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, f"{COUNTRY.lower()}_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

DEBUG = "--debug" in sys.argv

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

BLACKLIST = re.compile(
    r"\b(vaccine|vaccin|immunoglobulin|immunoglobul|albumin|dialys|"
    r"diagnostic|dispositif|device|veterinär|veterinary|radiopharm)\b", re.I
)
WITHDRAWN = re.compile(
    r"withdrawn|refused|suspended|expired|revoked|tilbagekaldt|"
    r"retirado|zurückgezogen|ritirato|wycofany|retiré|tilbaketrekk", re.I
)

def atc_category(atc):
    return ATC_MAP.get((atc or "").strip()[:3].upper())

def curl(url, dest, max_time=180):
    cmd = ["curl","-L","--max-time",str(max_time),"--connect-timeout","20",
           "--silent","--fail","--user-agent","Mozilla/5.0 apoHouze-updater/5.0",
           "-o", dest, url]
    for attempt in range(3):
        try:
            subprocess.run(cmd, timeout=max_time+15, check=True)
            size = os.path.getsize(dest)
            print(f"  ✅ {size//1024} KB")
            return size
        except Exception as e:
            print(f"  ⚠️  Poging {attempt+1}/3: {e}")
            if attempt < 2: time.sleep(4)
    return 0

def http_get_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 apoHouze-updater/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

# ──────────────────────────────────────────────
# BRON 1: EMA JSON (gedeeld voor alle landen)
# ──────────────────────────────────────────────
def fetch_ema(seen):
    print("  📥 EMA JSON...")
    dest = os.path.join(TMP_DIR, "shared_ema.json")
    # Hergebruik als recent gecached (< 4 uur oud)
    if os.path.exists(dest) and (time.time() - os.path.getmtime(dest)) < 14400:
        print("  ♻️  Gecachede EMA-data gebruiken")
    else:
        size = curl("https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json", dest)
        if size < 10000:
            print("  ⚠️  EMA JSON niet beschikbaar"); return []

    with open(dest, encoding="utf-8-sig") as f:
        raw = json.load(f)
    items = raw if isinstance(raw, list) else next((v for v in raw.values() if isinstance(v,list)), [])
    print(f"  📊 {len(items)} EMA-records")

    results, sk = [], 0
    keys = list(items[0].keys()) if items else []
    def find(*pats):
        for k in keys:
            kl = k.lower().replace(" ","_")
            if any(re.search(p,kl) for p in pats): return k
        return None
    name_k   = find(r"^medicine_name$", r"^name$", r"product")
    inn_k    = find(r"active_substance", r"\binn\b")
    atc_k    = find(r"^atc")
    status_k = find(r"authoris", r"status")

    for item in items:
        name   = str(item.get(name_k) or "").strip()
        if not name: continue
        status = str(item.get(status_k) or "") if status_k else ""
        if status and WITHDRAWN.search(status): sk+=1; continue
        if BLACKLIST.search(name): sk+=1; continue
        atc = str(item.get(atc_k) or "").strip() if atc_k else ""
        inn = str(item.get(inn_k) or "").strip() if inn_k else ""
        cat = atc_category(atc)
        if not cat: sk+=1; continue
        key = name.lower()
        if key in seen: continue
        seen.add(key)
        results.append({"Name":name,"INN":inn,"ATC":atc,"PharmaceuticalForm":"","RxStatus":"Rx","Country":COUNTRY})
    print(f"  ✅ {len(results)} EMA | {sk} overgeslagen")
    return results

# ──────────────────────────────────────────────
# BRON 2: Landspecifieke bronnen
# ──────────────────────────────────────────────

def fetch_es(seen):
    """Spanje — AEMPS CIMA REST API (officieel, open, geen auth vereist)"""
    print("  📥 AEMPS CIMA (Spanje)...")
    results = []
    # CIMA pagineert: max 25 per aanroep, gebruik pagina-loop
    page = 1
    while True:
        url = f"https://cima.aemps.es/cima/rest/medicamentos?estado=1&pageSize=100&numPagina={page}"
        try:
            data = http_get_json(url)
        except Exception as e:
            print(f"  ⚠️  CIMA pagina {page}: {e}"); break
        items = data.get("resultados", [])
        if not items: break
        for m in items:
            name = (m.get("nombre") or "").strip()
            inn  = (m.get("principiosActivos") or [{}])[0].get("nombre","") if m.get("principiosActivos") else ""
            atc  = (m.get("atcs") or [{}])[0].get("codigo","") if m.get("atcs") else ""
            form = (m.get("formaFarmaceutica") or {}).get("nombre","")
            if not name or BLACKLIST.search(name): continue
            cat = atc_category(atc)
            if not cat: continue
            key = name.lower()
            if key in seen: continue
            seen.add(key)
            results.append({"Name":name,"INN":inn,"ATC":atc,"PharmaceuticalForm":form,"RxStatus":"Rx","Country":"ES"})
        total = data.get("totalFilas",0)
        if page * 100 >= total: break
        page += 1
        time.sleep(0.3)  # beleefd pagineren
    print(f"  ✅ {len(results)} CIMA-producten")
    return results

def fetch_se(seen):
    """Zweden — Läkemedelsverket product list CSV"""
    print("  📥 Läkemedelsverket (Zweden)...")
    dest = os.path.join(TMP_DIR, "se_lv.csv")
    urls = [
        "https://www.lakemedelsverket.se/globalassets/produkt-och-tillstand/lakemedelsregistret/lakemedel-godkanda-for-forsaljning-i-sverige.csv",
        "https://www.lakemedelsverket.se/globalassets/produkt-och-tillstand/lakemedelsregistret/produkter.csv",
    ]
    ok = False
    for url in urls:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            ok = True; break
    if not ok:
        print("  ⚠️  SE-bron niet bereikbaar"); return []
    return parse_generic_csv(dest, "SE", seen,
        name_patterns=[r"produktnamn|läkemedel.*namn|name"],
        inn_patterns=[r"substans|aktiv.*ingred|inn"],
        atc_patterns=[r"^atc"],
        form_patterns=[r"läkemedels.*form|pharmaceutical.*form"],
        status_patterns=[r"status|godkänn"],
        rx_patterns=[r"recept|förskriv"])

def fetch_no(seen):
    """Noorwegen — NoMA legemiddelsok open data"""
    print("  📥 NoMA (Noorwegen)...")
    dest = os.path.join(TMP_DIR, "no_noma.csv")
    urls = [
        "https://www.legemiddelsok.no/sitecore/api/ssc/legemiddel-api/legemiddel/export?format=csv",
        "https://opendata.legemiddelverket.no/medisin/export.csv",
        "https://www.felleskatalogen.no/medisin/atc-register/export.csv",
    ]
    for url in urls:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            return parse_generic_csv(dest, "NO", seen,
                name_patterns=[r"varenavn|produktnavn|name"],
                inn_patterns=[r"virkestoff|substans|inn"],
                atc_patterns=[r"^atc"],
                form_patterns=[r"legemiddelform|form"],
                status_patterns=[r"status|markedsf"],
                rx_patterns=[r"resept|rekvis"])
        print(f"  ⚠️  Niet bereikbaar")
    return []

def fetch_ch(seen):
    """Zwitserland — Swissmedic authorised medicinal products"""
    print("  📥 Swissmedic (Zwitserland)...")
    dest = os.path.join(TMP_DIR, "ch_swissmedic.xlsx")
    urls = [
        "https://www.swissmedic.ch/swissmedic/de/home/services/listen_neu/zugelassene-humans-arzneimittel.html.downloadliste.xlsx",
        "https://www.swissmedic.ch/dam/swissmedic/de/dokumente/internetlisten/zugelassene-humans-arzneimittel.xlsx.download.xlsx/Zugelassene_Humanarzneimittel.xlsx",
        "https://www.swissmedic.ch/swissmedic/de/home/humanarzneimittel/authorisation/list-authorised-human-medicinal-products.html.downloadliste.xlsx",
    ]
    for url in urls:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            return parse_xlsx(dest, "CH", seen,
                name_patterns=[r"zulassungs.*bezeichnung|name|handelsname|bezeichnung"],
                inn_patterns=[r"wirkstoff|substanz|inn"],
                atc_patterns=[r"^atc"],
                form_patterns=[r"darreich|form"],
                status_patterns=[r"status|zulass"],
                rx_patterns=[r"abgabe|verschreib|rezept"])
        print(f"  ⚠️  Niet bereikbaar")
    return []

def fetch_at(seen):
    """Oostenrijk — BASG/AGES Arzneispezialitätenregister via data.gv.at"""
    print("  📥 BASG (Oostenrijk)...")
    # data.gv.at CKAN API
    dest = os.path.join(TMP_DIR, "at_basg.csv")
    urls = [
        # AGES Arzneispezialitätenregister open data
        "https://www.data.gv.at/katalog/api/3/action/package_show?id=ages-arzneispezialitaten",
    ]
    # Probeer CKAN API
    try:
        data = http_get_json("https://www.data.gv.at/katalog/api/3/action/package_show?id=ages-arzneispezialitaten")
        resources = data.get("result",{}).get("resources",[])
        csv_resources = [r for r in resources if r.get("format","").upper() in ("CSV","XLSX") or ".csv" in r.get("url","").lower()]
        if csv_resources:
            url = csv_resources[0]["url"]
            print(f"  🔗 {url}")
            if curl(url, dest) > 5000:
                return parse_generic_csv(dest, "AT", seen,
                    name_patterns=[r"bezeichnung|name|handelsname|zulassungs"],
                    inn_patterns=[r"wirkstoff|substanz|inn"],
                    atc_patterns=[r"^atc"],
                    form_patterns=[r"darreich|form"],
                    status_patterns=[r"status|zulass"],
                    rx_patterns=[r"abgabe|verschreib|rezept"])
    except Exception as e:
        print(f"  ⚠️  CKAN AT: {e}")
    # Directe fallback
    for url in [
        "https://www.ages.at/download/0/0/9ad5ba6c2c89c025da7a53f3ae84a77e4d5de67c/fileadmin/AGES2015/Infos/Humanarzneimittel_2024.xlsx",
        "https://www.basg.gv.at/fileadmin/user_upload/arzneispezialitaeten.csv",
    ]:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            return parse_generic_csv(dest, "AT", seen,
                name_patterns=[r"bezeichnung|name|handelsname"],
                inn_patterns=[r"wirkstoff|substanz"],
                atc_patterns=[r"^atc"],
                form_patterns=[r"darreich|form"],
                status_patterns=[r"status|zulass"],
                rx_patterns=[r"abgabe|verschreib"])
        print(f"  ⚠️  Niet bereikbaar")
    return []

def fetch_dk(seen):
    """Denemarken — DKMA medicinpriser open data"""
    print("  📥 DKMA (Denemarken)...")
    dest = os.path.join(TMP_DIR, "dk_dkma.csv")
    urls = [
        "https://www.medicinpriser.dk/default.aspx?action=downloadfile&file=medicineprices.csv",
        "https://laegemiddelstyrelsen.dk/en/medicines/authorised-medicines/authorised-human-medicines/download-list/",
        "https://medicinpriser.dk/default.aspx?action=export",
    ]
    for url in urls:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            return parse_generic_csv(dest, "DK", seen,
                name_patterns=[r"produktnavn|varenavn|name"],
                inn_patterns=[r"substans|virkestof|inn"],
                atc_patterns=[r"^atc"],
                form_patterns=[r"form"],
                status_patterns=[r"status|godkend"],
                rx_patterns=[r"recept"])
        print(f"  ⚠️  Niet bereikbaar")
    return []

def fetch_fi(seen):
    """Finland — Fimea open data (avoindata.fi)"""
    print("  📥 Fimea (Finland)...")
    dest = os.path.join(TMP_DIR, "fi_fimea.csv")
    # Probeer Fimea CKAN API via avoindata.fi
    urls_to_try = []
    try:
        data = http_get_json("https://www.avoindata.fi/api/3/action/package_show?id=laakkeet")
        resources = data.get("result",{}).get("resources",[])
        urls_to_try = [r["url"] for r in resources if ".csv" in r.get("url","").lower() or r.get("format","").upper()=="CSV"]
    except Exception as e:
        print(f"  ⚠️  Fimea CKAN: {e}")
    urls_to_try += [
        "https://www.fimea.fi/documents/160140/753095/Lääketieto_avoimena_datana_CSV.csv",
        "https://avoindata.fi/data/fi/dataset/laakkeet/resource/download",
    ]
    for url in urls_to_try:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            return parse_generic_csv(dest, "FI", seen,
                name_patterns=[r"kauppanimi|nimi|name"],
                inn_patterns=[r"vaikuttava.*aine|substans|inn"],
                atc_patterns=[r"^atc"],
                form_patterns=[r"lääkemuoto|form"],
                status_patterns=[r"status|myynti"],
                rx_patterns=[r"resepti|toimitus"])
        print(f"  ⚠️  Niet bereikbaar")
    return []

def fetch_ie(seen):
    """Ierland — HPRA (Irish Health Products Regulatory Authority)
    Gebruikt BNF-achtige data via HPRA open data of data.gov.ie CKAN"""
    print("  📥 HPRA (Ierland)...")
    dest = os.path.join(TMP_DIR, "ie_hpra.csv")
    # data.gov.ie CKAN
    try:
        data = http_get_json("https://data.gov.ie/api/3/action/package_search?q=hpra+medicines&rows=5")
        pkgs = data.get("result",{}).get("results",[])
        for pkg in pkgs:
            for r in pkg.get("resources",[]):
                if ".csv" in r.get("url","").lower() or r.get("format","").upper()=="CSV":
                    url = r["url"]
                    print(f"  🔗 {url}")
                    if curl(url, dest) > 5000:
                        return parse_generic_csv(dest, "IE", seen,
                            name_patterns=[r"product.*name|medicine.*name|name"],
                            inn_patterns=[r"active.*ingred|inn|substance"],
                            atc_patterns=[r"^atc"],
                            form_patterns=[r"pharmaceutical.*form|form"],
                            status_patterns=[r"status|authoris"],
                            rx_patterns=[r"prescription|supply"])
    except Exception as e:
        print(f"  ⚠️  HPRA CKAN: {e}")
    # Directe URLs
    for url in [
        "https://www.hpra.ie/docs/default-source/default-document-library/medicines-authorised.csv",
        "https://www.hpra.ie/homepage/medicines/medicines-information/find-a-medicine/export.csv",
    ]:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            return parse_generic_csv(dest, "IE", seen,
                name_patterns=[r"product|name|medicine"],
                inn_patterns=[r"active|inn|substance"],
                atc_patterns=[r"^atc"],
                form_patterns=[r"form"],
                status_patterns=[r"status|authoris"],
                rx_patterns=[r"prescription|supply"])
        print(f"  ⚠️  Niet bereikbaar")
    return []

def fetch_pt(seen):
    """Portugal — INFARMED open data"""
    print("  📥 INFARMED (Portugal)...")
    dest = os.path.join(TMP_DIR, "pt_infarmed.csv")
    urls = [
        "https://www.infarmed.pt/documents/15786/17838/infomed_dump.zip",
        "https://extranet.infarmed.pt/INFOMED-fo/download-ficheiro.xhtml?fileTypeCode=IMED_DUMP",
        "https://www.infarmed.pt/infomed/download_ficheiro.php?tipo=todosAIM",
    ]
    for url in urls:
        print(f"  📥 {url}")
        size = curl(url, dest)
        if size > 5000:
            # Controleer of het een ZIP is
            if url.endswith(".zip") or dest.endswith(".zip"):
                import zipfile
                try:
                    with zipfile.ZipFile(dest) as z:
                        names = z.namelist()
                        csv_name = next((n for n in names if n.lower().endswith(".csv") or n.lower().endswith(".txt")), names[0] if names else None)
                        if csv_name:
                            real_dest = os.path.join(TMP_DIR, "pt_infarmed_inner.csv")
                            with z.open(csv_name) as zf, open(real_dest,"wb") as f:
                                f.write(zf.read())
                            dest = real_dest
                except Exception:
                    pass
            return parse_generic_csv(dest, "PT", seen,
                name_patterns=[r"nome.*medicamento|denominação|nome.*comerc|name"],
                inn_patterns=[r"denomin.*comum|substância.*ativa|dci|inn"],
                atc_patterns=[r"^atc"],
                form_patterns=[r"forma.*farmac|form"],
                status_patterns=[r"estado|situação|status"],
                rx_patterns=[r"prescrição|receita|regime"])
        print(f"  ⚠️  Niet bereikbaar ({size}B)")
    return []

def fetch_pl(seen):
    """Polen — URPL Rejestr Produktów Leczniczych"""
    print("  📥 URPL (Polen)...")
    dest = os.path.join(TMP_DIR, "pl_urpl.csv")
    urls = [
        # rejestry.ezdrowie.gov.pl heeft een export-endpoint
        "https://rejestry.ezdrowie.gov.pl/api/rpl/medicinal-products/all-wp/export/csv",
        "https://rejestry.ezdrowie.gov.pl/rpl/api/public/v1/medicinal-products/export.csv",
        "https://www.urpl.gov.pl/pl/produkty-lecznicze/rejestr-produktow-leczniczych/eksport",
    ]
    for url in urls:
        print(f"  📥 {url}")
        if curl(url, dest) > 5000:
            return parse_generic_csv(dest, "PL", seen,
                name_patterns=[r"nazwa.*produktu|nazwa.*handl|name"],
                inn_patterns=[r"substancja.*czynna|inn|substancja"],
                atc_patterns=[r"^atc|kod.*atc"],
                form_patterns=[r"postać|forma"],
                status_patterns=[r"status|pozwolenie"],
                rx_patterns=[r"recepta|przepis"])
        print(f"  ⚠️  Niet bereikbaar")
    return []

# ──────────────────────────────────────────────
# HULPFUNCTIES: generieke CSV/XLSX parser
# ──────────────────────────────────────────────

def find_col(sample_row, patterns):
    for k in sample_row.keys():
        kl = k.lower().strip()
        if any(re.search(p, kl) for p in patterns):
            return k
    return None

def process_rows(rows, country, seen,
                 name_k, inn_k, atc_k, form_k, status_k, rx_k):
    results, sk_bl, sk_cat, sk_status, sk_dup = [], 0, 0, 0, 0
    for row in rows:
        name   = str(row.get(name_k) or "").strip() if name_k else ""
        inn    = str(row.get(inn_k) or "").strip()  if inn_k  else ""
        atc    = str(row.get(atc_k) or "").strip()  if atc_k  else ""
        form   = str(row.get(form_k) or "").strip() if form_k else ""
        status = str(row.get(status_k) or "").strip() if status_k else ""
        rx_raw = str(row.get(rx_k) or "").strip()   if rx_k   else ""
        if not name and not inn:
            continue
        display = name or inn
        if BLACKLIST.search(display): sk_bl+=1; continue
        if status and WITHDRAWN.search(status): sk_status+=1; continue
        cat = atc_category(atc)
        if not cat: sk_cat+=1; continue
        rx = bool(re.search(r"recept|prescri|liste[_ ]?i|verschreib|resepti", rx_raw, re.I))
        key = display.lower()
        if key in seen: sk_dup+=1; continue
        seen.add(key)
        results.append({"Name":display,"INN":inn,"ATC":atc,
                        "PharmaceuticalForm":form,
                        "RxStatus":"Rx" if rx else "OTC","Country":country})
    if DEBUG:
        print(f"  📊 {len(results)} | bl:{sk_bl} cat:{sk_cat} st:{sk_status} dup:{sk_dup}")
    return results

def parse_generic_csv(path, country, seen,
                      name_patterns, inn_patterns, atc_patterns,
                      form_patterns, status_patterns, rx_patterns):
    for enc in ("utf-8-sig","latin-1","utf-8","cp1250"):
        try:
            with open(path, encoding=enc, errors="strict") as f:
                sample = f.read(8192); f.seek(0)
                sep = ";" if sample.count(";") > sample.count(",") else ","
                if sample.count("\t") > sample.count(sep): sep = "\t"
                reader = csv.DictReader(f, delimiter=sep)
                rows = list(reader)
            if not rows: return []
            if DEBUG: print(f"  🔍 {path}: {enc}, sep='{sep}', cols={list(rows[0].keys())[:6]}")
            print(f"  📊 {len(rows)} rijen geladen")
            s = rows[0]
            nk  = find_col(s, name_patterns)
            ik  = find_col(s, inn_patterns)
            ak  = find_col(s, atc_patterns)
            fk  = find_col(s, form_patterns)
            stk = find_col(s, status_patterns)
            rxk = find_col(s, rx_patterns)
            if not nk and not ik:
                print(f"  ⚠️  Geen naam/INN kolom gevonden. Kolommen: {list(s.keys())[:8]}")
                return []
            return process_rows(rows, country, seen, nk, ik, ak, fk, stk, rxk)
        except UnicodeDecodeError:
            continue
    return []

def parse_xlsx(path, country, seen,
               name_patterns, inn_patterns, atc_patterns,
               form_patterns, status_patterns, rx_patterns):
    try:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        rows_raw = list(ws.iter_rows(values_only=True))
        wb.close()
        if not rows_raw: return []
        headers = [str(h).strip() if h else f"col{i}" for i,h in enumerate(rows_raw[0])]
        rows = [{headers[i]: (str(rows_raw[r][i]).strip() if i < len(rows_raw[r]) and rows_raw[r][i] is not None else "")
                 for i in range(len(headers))} for r in range(1, len(rows_raw))]
        print(f"  📊 {len(rows)} rijen geladen (xlsx)")
        if not rows: return []
        s = rows[0]
        nk  = find_col(s, name_patterns)
        ik  = find_col(s, inn_patterns)
        ak  = find_col(s, atc_patterns)
        fk  = find_col(s, form_patterns)
        stk = find_col(s, status_patterns)
        rxk = find_col(s, rx_patterns)
        return process_rows(rows, country, seen, nk, ik, ak, fk, stk, rxk)
    except Exception as e:
        print(f"  ⚠️  XLSX fout: {e}")
        return []

# ──────────────────────────────────────────────
# DISPATCHER
# ──────────────────────────────────────────────
COUNTRY_FETCHERS = {
    "ES": fetch_es,
    "SE": fetch_se,
    "NO": fetch_no,
    "CH": fetch_ch,
    "AT": fetch_at,
    "DK": fetch_dk,
    "FI": fetch_fi,
    "IE": fetch_ie,
    "PT": fetch_pt,
    "PL": fetch_pl,
}

def main():
    flag = {
        "AT":"🇦🇹","CH":"🇨🇭","DK":"🇩🇰","ES":"🇪🇸","FI":"🇫🇮",
        "IE":"🇮🇪","NO":"🇳🇴","PL":"🇵🇱","PT":"🇵🇹","SE":"🇸🇪",
    }.get(COUNTRY, "🌍")
    print(f"{flag} apoHouze — {COUNTRY} Medicijnen Fetcher")
    print("=" * 50)

    if COUNTRY not in COUNTRY_FETCHERS and COUNTRY not in ("DE","FR","IT","GB","NL","BE","US","CA"):
        print(f"❌ Landcode '{COUNTRY}' niet ondersteund.")
        sys.exit(1)

    seen = set()
    all_results = []

    # Stap 1: EMA JSON (gedeeld voor alle EU-landen)
    print("\n[1/2] EMA JSON (centraal vergunde EU-middelen)...")
    ema = fetch_ema(seen)
    all_results.extend(ema)

    # Stap 2: Landspecifieke bron
    fetcher = COUNTRY_FETCHERS.get(COUNTRY)
    if fetcher:
        print(f"\n[2/2] Nationale bron ({COUNTRY})...")
        national = fetcher(seen)
        all_results.extend(national)
    else:
        print(f"\n[2/2] Geen extra nationale bron voor {COUNTRY}")

    print(f"\n  🎯 Totaal: {len(all_results)} unieke medicijnen")
    if not all_results:
        print("❌ Geen resultaten"); sys.exit(1)

    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(all_results)
    print(f"✅ {len(all_results)} opgeslagen → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
