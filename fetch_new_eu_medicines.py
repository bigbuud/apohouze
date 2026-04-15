#!/usr/bin/env python3
"""
apoHouze — Nieuwe EU-landen Fetcher v2
=======================================
Gebruik: python3 fetch_new_eu_medicines.py <landcode>
Landen:  CZ SK HR SI HU RO GR LU

Strategie:
  1. EMA JSON  (zelfde werkende URL als fetch_eu_medicines.py)
  2. Nationale bron via curl
  3. Hardcoded fallback (bestand is NOOIT leeg)

Output: data/_tmp/<code>_medicines.csv
Kolomnamen: name, generic, atc, pharmaceutical_form, rx, status
  -> update.js parseFile() herkent deze automatisch
"""
import sys, os, re, csv, json, io, time, subprocess, zipfile

if len(sys.argv) < 2:
    print(f"Gebruik: python3 {sys.argv[0]} <landcode>"); sys.exit(1)

COUNTRY   = sys.argv[1].upper()
REPO_ROOT = os.getcwd()
TMP_DIR   = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT    = os.path.join(TMP_DIR, f"{COUNTRY.lower()}_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

VALID = {"CZ","SK","HR","SI","HU","RO","GR","LU"}
if COUNTRY not in VALID:
    print(f"Onbekend: {COUNTRY}. Kies uit: {', '.join(sorted(VALID))}"); sys.exit(1)

ATC_MAP = {
    "A02":"Stomach & Intestine","A03":"Stomach & Intestine","A04":"Stomach & Intestine",
    "A05":"Stomach & Intestine","A06":"Stomach & Intestine","A07":"Stomach & Intestine",
    "A08":"Stomach & Intestine","A09":"Stomach & Intestine","A10":"Diabetes",
    "A11":"Vitamins & Supplements","A12":"Vitamins & Supplements","A13":"Vitamins & Supplements",
    "A16":"Stomach & Intestine","B01":"Anticoagulants","B02":"Heart & Blood Pressure",
    "B03":"Vitamins & Supplements","B05":"Heart & Blood Pressure","B06":"Heart & Blood Pressure",
    "C01":"Heart & Blood Pressure","C02":"Heart & Blood Pressure","C03":"Heart & Blood Pressure",
    "C04":"Heart & Blood Pressure","C05":"Heart & Blood Pressure","C07":"Heart & Blood Pressure",
    "C08":"Heart & Blood Pressure","C09":"Heart & Blood Pressure","C10":"Cholesterol",
    "D01":"Antifungals","D02":"Skin & Wounds","D03":"Skin & Wounds","D04":"Skin & Wounds",
    "D05":"Skin & Wounds","D06":"Antibiotics","D07":"Corticosteroids","D08":"Skin & Wounds",
    "D09":"Skin & Wounds","D10":"Skin & Wounds","D11":"Skin & Wounds",
    "G01":"Women's Health","G02":"Women's Health","G03":"Women's Health","G04":"Urology",
    "H01":"Thyroid","H02":"Corticosteroids","H03":"Thyroid","H04":"Diabetes",
    "H05":"Vitamins & Supplements","J01":"Antibiotics","J02":"Antifungals",
    "J04":"Antibiotics","J05":"Antivirals","J06":"Antivirals","J07":"Antivirals",
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
WITHDRAWN = re.compile(r"withdrawn|ingetrokken|wycofan|stažen|visszavon|retras|ανακλ", re.I)

def atc_cat(atc):
    return ATC_MAP.get((atc or "").strip()[:3].upper(), "")

def curl(url, dest, max_time=120):
    cmd = ["curl","-L","--max-time",str(max_time),"--connect-timeout","20",
           "--silent","--fail","-A","Mozilla/5.0 apoHouze-updater/5.0","-o",dest,url]
    try:
        subprocess.run(cmd, check=True, timeout=max_time+15)
        return os.path.getsize(dest) if os.path.exists(dest) else 0
    except Exception as e:
        print(f"    curl: {e}"); return 0

def is_html(p):
    try:
        with open(p,"rb") as f: return b"<html" in f.read(512).lower()
    except: return True

# ── EMA JSON (zelfde URL als fetch_eu_medicines.py) ──────────────
EMA_CACHE = os.path.join(TMP_DIR, "shared_ema.json")
EMA_URL   = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json"

def fetch_ema(seen):
    print("  📥 EMA JSON...")
    if os.path.exists(EMA_CACHE) and (time.time()-os.path.getmtime(EMA_CACHE))<14400:
        print("  ♻️  Cache")
    else:
        if curl(EMA_URL, EMA_CACHE) < 10000:
            print("    EMA niet beschikbaar"); return []
    try:
        with open(EMA_CACHE, encoding="utf-8-sig") as f: raw = json.load(f)
    except Exception as e:
        print(f"    EMA parse: {e}"); return []
    items = raw if isinstance(raw,list) else next((v for v in raw.values() if isinstance(v,list)),[])
    if not items: return []
    keys = list(items[0].keys())
    def find(*pats):
        for k in keys:
            kl = k.lower().replace(" ","_")
            if any(re.search(p,kl) for p in pats): return k
        return None
    nk = find(r"medicine.*name",r"name_of_medicine",r"^name$")
    ik = find(r"active_substance",r"\binn\b")
    ak = find(r"^atc")
    sk = find(r"authoris",r"status")
    results, skipped = [], 0
    for item in items:
        name = str(item.get(nk) or "").strip() if nk else ""
        if not name: continue
        status = str(item.get(sk) or "") if sk else ""
        if WITHDRAWN.search(status): skipped+=1; continue
        atc = str(item.get(ak) or "").strip() if ak else ""
        inn = str(item.get(ik) or "").strip() if ik else ""
        if not atc_cat(atc): skipped+=1; continue
        key = name.lower()
        if key in seen: continue
        seen.add(key)
        results.append({"name":name,"generic":inn,"atc":atc,"pharmaceutical_form":"","rx":"Rx","status":status})
    print(f"  ✅ EMA: {len(results)} | overgeslagen: {skipped}")
    return results

# ── Nationale xlsx helper ─────────────────────────────────────────
def parse_xlsx(path, seen, name_cols, inn_cols, atc_col="ATC", form_col=None, skip_col=None, skip_re=None):
    rows = []
    try:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True); ws = wb.active
        hdrs = None
        for row in ws.iter_rows(values_only=True):
            if hdrs is None:
                hdrs = [str(c or "").strip() for c in row]
                print(f"    Kolommen: {hdrs[:10]}"); continue
            rd = dict(zip(hdrs,[str(c or "") for c in row]))
            name = next((rd[c].strip() for c in name_cols if rd.get(c,"").strip()),"")
            if not name: continue
            if skip_col and skip_re and re.search(skip_re, rd.get(skip_col,"").lower()): continue
            inn  = next((rd[c].strip() for c in inn_cols  if rd.get(c,"").strip()),"")
            atc  = rd.get(atc_col,"").strip()
            form = rd.get(form_col,"").strip() if form_col else ""
            key  = name.lower()
            if key in seen: continue
            seen.add(key); rows.append({"name":name,"generic":inn,"atc":atc,"pharmaceutical_form":form,"rx":"Rx","status":"authorised"})
    except ImportError: print("    openpyxl ontbreekt")
    except Exception as e: print(f"    xlsx: {e}")
    print(f"    Nationaal xlsx: {len(rows)} records")
    return rows

def try_xlsx(urls, seen, name_cols, inn_cols, atc_col="ATC", form_col=None, skip_col=None, skip_re=None):
    dest = os.path.join(TMP_DIR, f"{COUNTRY.lower()}_nat.xlsx")
    for url in urls:
        print(f"    Probeer {url}")
        if curl(url, dest, 90) > 10000 and not is_html(dest):
            return parse_xlsx(dest, seen, name_cols, inn_cols, atc_col, form_col, skip_col, skip_re)
    print("    Nationale bron niet bereikbaar"); return []

# ── CZ: SÚKL ZIP ─────────────────────────────────────────────────
def national_CZ(seen):
    dest = os.path.join(TMP_DIR,"cz_nat.zip")
    for url in ["https://opendata.sukl.cz/soubory/DLP.zip","https://opendata.sukl.cz/soubory/KOD_SUKL.zip"]:
        if curl(url, dest, 120) > 10000: break
    else: return []
    rows=[]
    try:
        with zipfile.ZipFile(dest) as z:
            csvs=[f for f in z.namelist() if f.lower().endswith(".csv")]
            print(f"    ZIP: {csvs[:3]}")
            for cn in csvs[:2]:
                with z.open(cn) as f: content=f.read().decode("utf-8-sig",errors="replace")
                sep=";" if ";" in content.splitlines()[0] else ","
                for rd in csv.DictReader(io.StringIO(content),delimiter=sep):
                    name=str(rd.get("NAZEV","") or rd.get("Název","") or "").strip()
                    if not name: continue
                    if re.search(r"zruš|revok|withdr",str(rd.get("STAV","")).lower()): continue
                    key=name.lower()
                    if key in seen: continue
                    seen.add(key)
                    rows.append({"name":name,"generic":str(rd.get("INN","")).strip(),"atc":str(rd.get("ATC","")).strip(),"pharmaceutical_form":str(rd.get("FORMA","")).strip(),"rx":"Rx","status":"authorised"})
    except Exception as e: print(f"    ZIP: {e}")
    print(f"    SÚKL: {len(rows)}"); return rows

def national_SK(seen):
    return try_xlsx(["https://www.sukl.sk/buxus/docs/lieky/register_liekov.xlsx"],seen,
        name_cols=["Názov lieku","Obchodný názov"],inn_cols=["INN","Účinná látka"],
        atc_col="ATC",form_col="Lieková forma",skip_col="Stav",skip_re=r"zrušen|withdr")

def national_HR(seen):
    return try_xlsx(["https://www.halmed.hr/upl/lijekovi/registar_lijekova.xlsx",
                     "https://www.halmed.hr/upl/lijekovi/odobreni_lijekovi.xlsx"],seen,
        name_cols=["Naziv lijeka","Ime lijeka"],inn_cols=["INN","Djelatna tvar"],
        atc_col="ATC",form_col="Farmaceutski oblik")

def national_SI(seen):
    return try_xlsx(["https://www.jazmp.si/fileadmin/datoteke/baza_zdravil/baza_zdravil.xlsx"],seen,
        name_cols=["Ime zdravila","Naziv zdravila"],inn_cols=["INN","Učinkovina"],
        atc_col="ATC",form_col="Farmacevtska oblika")

def national_HU(seen):
    return try_xlsx(["https://www.ogyei.gov.hu/gyogyszeradatbazis/download/engedely.xlsx"],seen,
        name_cols=["Készítmény neve","Gyógyszer neve"],inn_cols=["INN","Hatóanyag"],
        atc_col="ATC",form_col="Gyógyszerforma")

def national_RO(seen):
    return try_xlsx(["https://anm.ro/wp-content/uploads/lista_medicamente.xlsx"],seen,
        name_cols=["Denumire comerciala","Denumire"],inn_cols=["DCI","INN","Substanta activa"],
        atc_col="Cod ATC",form_col="Forma farmaceutica")

def national_GR(seen):
    return try_xlsx(["https://www.eof.gr/c/document_library/get_file?groupId=21839&folderId=21841&name=DLFE-approved.xlsx"],seen,
        name_cols=["Εμπορική Ονομασία","Trade name","Name"],inn_cols=["INN","Δραστική Ουσία"],
        atc_col="ATC",form_col="Φαρμακοτεχνική Μορφή")

def national_LU(seen): return []

NATIONAL = {"CZ":national_CZ,"SK":national_SK,"HR":national_HR,"SI":national_SI,
            "HU":national_HU,"RO":national_RO,"GR":national_GR,"LU":national_LU}


# ── Hardcoded fallback (garantie: nooit leeg) ─────────────────────
FALLBACK = {
"CZ":[
    ("Paralen 500mg tablety","Paracetamol","N02BE01","Tablet",False),
    ("Brufen 400mg tablety","Ibuprofen","M01AE01","Tablet",False),
    ("Nurofen 200mg tablety","Ibuprofen","M01AE01","Tablet",False),
    ("Ibalgin 400mg tablety","Ibuprofen","M01AE01","Tablet",False),
    ("Aspirin 500mg tablety","Aspirin","N02BA01","Tablet",False),
    ("Panadol 500mg tablety","Paracetamol","N02BE01","Tablet",False),
    ("Voltaren Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Diclofenac AL 50mg tablety","Diclofenac","M01AB05","Tablet",True),
    ("Zyrtec 10mg tablety","Cetirizine","R06AE07","Tablet",False),
    ("Zodac 10mg tablety","Cetirizine","R06AE07","Tablet",False),
    ("Claritine 10mg tablety","Loratadine","R06AX13","Tablet",False),
    ("Aerius 5mg tablety","Desloratadine","R06AX27","Tablet",True),
    ("Xylometazolin AL 0.1% nosní sprej","Xylometazoline","R01AA07","Nasal spray",False),
    ("Otrivin 0.1% nosní sprej","Xylometazoline","R01AA07","Nasal spray",False),
    ("ACC 200mg šumivé tablety","Acetylcysteine","R05CB01","Effervescent tablet",False),
    ("Mucosolvan 30mg sirup","Ambroxol","R05CB06","Syrup",False),
    ("Omeprazol AL 20mg tobolky","Omeprazole","A02BC01","Capsule",True),
    ("Helicid 20mg tobolky","Omeprazole","A02BC01","Capsule",True),
    ("Pantoprazol AL 40mg tablety","Pantoprazole","A02BC02","Tablet",True),
    ("Imodium 2mg tobolky","Loperamide","A07DA03","Capsule",False),
    ("No-Spa 40mg tablety","Drotaverine","A03AD02","Tablet",False),
    ("Atorvastatin AL 20mg tablety","Atorvastatin","C10AA05","Tablet",True),
    ("Simvastatin AL 20mg tablety","Simvastatin","C10AA01","Tablet",True),
    ("Amlodipine AL 5mg tablety","Amlodipine","C08CA01","Tablet",True),
    ("Bisoprolol AL 5mg tablety","Bisoprolol","C07AB07","Tablet",True),
    ("Ramipril AL 5mg tablety","Ramipril","C09AA05","Tablet",True),
    ("Losartan AL 50mg tablety","Losartan","C09CA01","Tablet",True),
    ("Furosemid AL 40mg tablety","Furosemide","C03CA01","Tablet",True),
    ("Metformin AL 1000mg tablety","Metformin","A10BA02","Tablet",True),
    ("Glimepirid AL 2mg tablety","Glimepiride","A10BB12","Tablet",True),
    ("Sertralin AL 50mg tablety","Sertraline","N06AB06","Tablet",True),
    ("Escitalopram AL 10mg tablety","Escitalopram","N06AB10","Tablet",True),
    ("Amoxicilin AL 500mg tobolky","Amoxicillin","J01CA04","Capsule",True),
    ("Augmentin 625mg tablety","Amoxicillin/Clavulanate","J01CR02","Tablet",True),
    ("Azithromycin AL 500mg tablety","Azithromycin","J01FA10","Tablet",True),
    ("Ciprinol 500mg tablety","Ciprofloxacin","J01MA02","Tablet",True),
    ("Levothyroxin AL 100mcg tablety","Levothyroxine","H03AA01","Tablet",True),
    ("Hydrocortison 1% krém","Hydrocortisone","D07AA02","Cream",False),
    ("Clotrimazol AL 1% krém","Clotrimazole","D01AC01","Cream",False),
    ("Salbutamol AL 100mcg inhalátor","Salbutamol","R03AC02","Inhaler",True),
    ("Zolpidem AL 10mg tablety","Zolpidem","N05CF02","Tablet",True),
    ("Vitamin D3 Zentiva 1000 IU","Cholecalciferol","A11CC05","Capsule",False),
    ("Calcium D3 Ratiopharm","Calcium/Vitamin D","A12AX","Tablet",False),
],
"SK":[
    ("Paralen 500mg tablety","Paracetamol","N02BE01","Tablet",False),
    ("Nurofen 200mg tablety","Ibuprofen","M01AE01","Tablet",False),
    ("Ibuprofen Sandoz 400mg tablety","Ibuprofen","M01AE01","Tablet",False),
    ("Aspirin 500mg tablety","Aspirin","N02BA01","Tablet",False),
    ("Voltaren Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Zyrtec 10mg tablety","Cetirizine","R06AE07","Tablet",False),
    ("Claritine 10mg tablety","Loratadine","R06AX13","Tablet",False),
    ("Aerius 5mg tablety","Desloratadine","R06AX27","Tablet",True),
    ("Otrivin 0.1% nosová aerodisperzia","Xylometazoline","R01AA07","Nasal spray",False),
    ("ACC 200mg šumivé tablety","Acetylcysteine","R05CB01","Effervescent tablet",False),
    ("Imodium 2mg kapsuly","Loperamide","A07DA03","Capsule",False),
    ("Omeprazol Stada 20mg kapsuly","Omeprazole","A02BC01","Capsule",True),
    ("Atorvastatin Stada 20mg tablety","Atorvastatin","C10AA05","Tablet",True),
    ("Amlodipín Stada 5mg tablety","Amlodipine","C08CA01","Tablet",True),
    ("Bisoprolol Stada 5mg tablety","Bisoprolol","C07AB07","Tablet",True),
    ("Ramipril Stada 5mg tablety","Ramipril","C09AA05","Tablet",True),
    ("Metformín Stada 1000mg tablety","Metformin","A10BA02","Tablet",True),
    ("Sertrán 50mg tablety","Sertraline","N06AB06","Tablet",True),
    ("Amoksicilin Stada 500mg kapsuly","Amoxicillin","J01CA04","Capsule",True),
    ("Azitromycín Stada 500mg tablety","Azithromycin","J01FA10","Tablet",True),
    ("Levothyroxin Stada 100mcg tablety","Levothyroxine","H03AA01","Tablet",True),
    ("Salbutamol Stada 100mcg inhalátor","Salbutamol","R03AC02","Inhaler",True),
    ("Hydrocortison 1% krém","Hydrocortisone","D07AA02","Cream",False),
    ("Clotrimazol 1% krém","Clotrimazole","D01AC01","Cream",False),
    ("Vitamin D3 1000 IU","Cholecalciferol","A11CC05","Capsule",False),
],
"HR":[
    ("Paracetamol PharmaS 500mg tablete","Paracetamol","N02BE01","Tablet",False),
    ("Nurofen 200mg tablete","Ibuprofen","M01AE01","Tablet",False),
    ("Ibuprofen PharmaS 400mg tablete","Ibuprofen","M01AE01","Tablet",False),
    ("Aspirin 500mg tablete","Aspirin","N02BA01","Tablet",False),
    ("Voltaren Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Diklofen 50mg tablete","Diclofenac","M01AB05","Tablet",True),
    ("Zyrtec 10mg tablete","Cetirizine","R06AE07","Tablet",False),
    ("Claritine 10mg tablete","Loratadine","R06AX13","Tablet",False),
    ("Aerius 5mg tablete","Desloratadine","R06AX27","Tablet",True),
    ("Xylometazolin 0.1% nazalni sprej","Xylometazoline","R01AA07","Nasal spray",False),
    ("ACC 200mg šumeće tablete","Acetylcysteine","R05CB01","Effervescent tablet",False),
    ("Imodium 2mg kapsule","Loperamide","A07DA03","Capsule",False),
    ("Omeprazol 20mg kapsule","Omeprazole","A02BC01","Capsule",True),
    ("Pantoprazol 40mg tablete","Pantoprazole","A02BC02","Tablet",True),
    ("Atorvastatin PharmaS 20mg tablete","Atorvastatin","C10AA05","Tablet",True),
    ("Amlodipin PharmaS 5mg tablete","Amlodipine","C08CA01","Tablet",True),
    ("Bisoprolol PharmaS 5mg tablete","Bisoprolol","C07AB07","Tablet",True),
    ("Ramipril PharmaS 5mg tablete","Ramipril","C09AA05","Tablet",True),
    ("Metformin PharmaS 1000mg tablete","Metformin","A10BA02","Tablet",True),
    ("Amoksicilin 500mg kapsule","Amoxicillin","J01CA04","Capsule",True),
    ("Augmentin 625mg tablete","Amoxicillin/Clavulanate","J01CR02","Tablet",True),
    ("Azitromicin 500mg tablete","Azithromycin","J01FA10","Tablet",True),
    ("Levotiroksin 100mcg tablete","Levothyroxine","H03AA01","Tablet",True),
    ("Salbutamol 100mcg inhaler","Salbutamol","R03AC02","Inhaler",True),
    ("Sertralin 50mg tablete","Sertraline","N06AB06","Tablet",True),
    ("Hidrokortizol 1% krema","Hydrocortisone","D07AA02","Cream",False),
    ("Klotrimazol 1% krema","Clotrimazole","D01AC01","Cream",False),
    ("Vitamin D3 1000 IJ","Cholecalciferol","A11CC05","Capsule",False),
    ("Zolpidem 10mg tablete","Zolpidem","N05CF02","Tablet",True),
],
"SI":[
    ("Lekadol 500mg tablete","Paracetamol","N02BE01","Tablet",False),
    ("Paracetamol Sandoz 500mg tablete","Paracetamol","N02BE01","Tablet",False),
    ("Nurofen 200mg tablete","Ibuprofen","M01AE01","Tablet",False),
    ("Ibuprofen Sandoz 400mg tablete","Ibuprofen","M01AE01","Tablet",False),
    ("Aspirin 500mg tablete","Aspirin","N02BA01","Tablet",False),
    ("Voltaren Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Diklofenak Sandoz 50mg tablete","Diclofenac","M01AB05","Tablet",True),
    ("Zyrtec 10mg tablete","Cetirizine","R06AE07","Tablet",False),
    ("Clarityne 10mg tablete","Loratadine","R06AX13","Tablet",False),
    ("Aerius 5mg tablete","Desloratadine","R06AX27","Tablet",True),
    ("Otrivin 0.1% pršilo za nos","Xylometazoline","R01AA07","Nasal spray",False),
    ("ACC 200mg šumeče tablete","Acetylcysteine","R05CB01","Effervescent tablet",False),
    ("Imodium 2mg kapsule","Loperamide","A07DA03","Capsule",False),
    ("Omeprazol Sandoz 20mg kapsule","Omeprazole","A02BC01","Capsule",True),
    ("Pantoprazol Sandoz 40mg tablete","Pantoprazole","A02BC02","Tablet",True),
    ("Atorvastatin Sandoz 20mg tablete","Atorvastatin","C10AA05","Tablet",True),
    ("Amlodipin Sandoz 5mg tablete","Amlodipine","C08CA01","Tablet",True),
    ("Bisoprolol Sandoz 5mg tablete","Bisoprolol","C07AB07","Tablet",True),
    ("Ramipril Sandoz 5mg tablete","Ramipril","C09AA05","Tablet",True),
    ("Metformin Sandoz 1000mg tablete","Metformin","A10BA02","Tablet",True),
    ("Amoksicilin Sandoz 500mg kapsule","Amoxicillin","J01CA04","Capsule",True),
    ("Azitromicin Sandoz 500mg tablete","Azithromycin","J01FA10","Tablet",True),
    ("Levotiroksin Sandoz 100mcg tablete","Levothyroxine","H03AA01","Tablet",True),
    ("Salbutamol 100mcg inhaler","Salbutamol","R03AC02","Inhaler",True),
    ("Sertralin Sandoz 50mg tablete","Sertraline","N06AB06","Tablet",True),
    ("Hidrokortizon 1% krema","Hydrocortisone","D07AA02","Cream",False),
    ("Klotrimazol 1% krema","Clotrimazole","D01AC01","Cream",False),
    ("Vitamin D3 1000 IE","Cholecalciferol","A11CC05","Capsule",False),
    ("Zolpidem Sandoz 10mg tablete","Zolpidem","N05CF02","Tablet",True),
    ("Kalcij + Vitamin D3 Sandoz","Calcium/Vitamin D","A12AX","Tablet",False),
],
"HU":[
    ("Paracetamol Nőbilin 500mg tabletta","Paracetamol","N02BE01","Tablet",False),
    ("Nurofen 200mg filmtabletta","Ibuprofen","M01AE01","Tablet",False),
    ("Ibuprofen AL 400mg filmtabletta","Ibuprofen","M01AE01","Tablet",False),
    ("Aspirin 500mg tabletta","Aspirin","N02BA01","Tablet",False),
    ("Voltaren Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Diklofenák AL 50mg filmtabletta","Diclofenac","M01AB05","Tablet",True),
    ("Zyrtec 10mg filmtabletta","Cetirizine","R06AE07","Tablet",False),
    ("Loratadin AL 10mg tabletta","Loratadine","R06AX13","Tablet",False),
    ("Aerius 5mg filmtabletta","Desloratadine","R06AX27","Tablet",True),
    ("Xylometazolin AL 0.1% orrspray","Xylometazoline","R01AA07","Nasal spray",False),
    ("ACC 200mg pezsgőtabletta","Acetylcysteine","R05CB01","Effervescent tablet",False),
    ("Imodium 2mg kemény kapszula","Loperamide","A07DA03","Capsule",False),
    ("Omeprazol AL 20mg gyomornedv-ellenálló kapszula","Omeprazole","A02BC01","Capsule",True),
    ("Pantoprazol AL 40mg tabletta","Pantoprazole","A02BC02","Tablet",True),
    ("Atorvastatin AL 20mg filmtabletta","Atorvastatin","C10AA05","Tablet",True),
    ("Simvastatin AL 20mg filmtabletta","Simvastatin","C10AA01","Tablet",True),
    ("Amlodipin AL 5mg tabletta","Amlodipine","C08CA01","Tablet",True),
    ("Bisoprolol AL 5mg filmtabletta","Bisoprolol","C07AB07","Tablet",True),
    ("Ramipril AL 5mg tabletta","Ramipril","C09AA05","Tablet",True),
    ("Losartan AL 50mg filmtabletta","Losartan","C09CA01","Tablet",True),
    ("Metformin AL 1000mg filmtabletta","Metformin","A10BA02","Tablet",True),
    ("Glimepirid AL 2mg tabletta","Glimepiride","A10BB12","Tablet",True),
    ("Amoxicillin AL 500mg kemény kapszula","Amoxicillin","J01CA04","Capsule",True),
    ("Augmentin 625mg filmtabletta","Amoxicillin/Clavulanate","J01CR02","Tablet",True),
    ("Azitromicin AL 500mg filmtabletta","Azithromycin","J01FA10","Tablet",True),
    ("Ciprofloxacin AL 500mg filmtabletta","Ciprofloxacin","J01MA02","Tablet",True),
    ("Levothyroxin AL 100mcg tabletta","Levothyroxine","H03AA01","Tablet",True),
    ("Salbutamol AL 100mcg inhalátor","Salbutamol","R03AC02","Inhaler",True),
    ("Sertralin AL 50mg filmtabletta","Sertraline","N06AB06","Tablet",True),
    ("Escitalopram AL 10mg filmtabletta","Escitalopram","N06AB10","Tablet",True),
    ("Hidrokortison 1% krém","Hydrocortisone","D07AA02","Cream",False),
    ("Klotrimazol AL 1% krém","Clotrimazole","D01AC01","Cream",False),
    ("D-vitamin 1000 NE","Cholecalciferol","A11CC05","Capsule",False),
    ("Magnézium-citrát tabletta","Magnesium","A12CC","Tablet",False),
    ("Zolpidem AL 10mg filmtabletta","Zolpidem","N05CF02","Tablet",True),
],
"RO":[
    ("Paracetamol Biofarm 500mg comprimate","Paracetamol","N02BE01","Tablet",False),
    ("Nurofen 200mg comprimate","Ibuprofen","M01AE01","Tablet",False),
    ("Ibuprofen Sandoz 400mg comprimate","Ibuprofen","M01AE01","Tablet",False),
    ("Aspirin 500mg comprimate","Aspirin","N02BA01","Tablet",False),
    ("Voltaren Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Diclofenac Sandoz 50mg comprimate","Diclofenac","M01AB05","Tablet",True),
    ("Zyrtec 10mg comprimate","Cetirizine","R06AE07","Tablet",False),
    ("Loratadina Sandoz 10mg comprimate","Loratadine","R06AX13","Tablet",False),
    ("Aerius 5mg comprimate","Desloratadine","R06AX27","Tablet",True),
    ("Xilometazolina 0.1% spray nazal","Xylometazoline","R01AA07","Nasal spray",False),
    ("ACC 200mg comprimate efervescente","Acetylcysteine","R05CB01","Effervescent tablet",False),
    ("Imodium 2mg capsule","Loperamide","A07DA03","Capsule",False),
    ("Omeprazol Sandoz 20mg capsule","Omeprazole","A02BC01","Capsule",True),
    ("Pantoprazol Sandoz 40mg comprimate","Pantoprazole","A02BC02","Tablet",True),
    ("Atorvastatina Sandoz 20mg comprimate","Atorvastatin","C10AA05","Tablet",True),
    ("Simvastatina Sandoz 20mg comprimate","Simvastatin","C10AA01","Tablet",True),
    ("Amlodipina Sandoz 5mg comprimate","Amlodipine","C08CA01","Tablet",True),
    ("Bisoprolol Sandoz 5mg comprimate","Bisoprolol","C07AB07","Tablet",True),
    ("Ramipril Sandoz 5mg comprimate","Ramipril","C09AA05","Tablet",True),
    ("Losartan Sandoz 50mg comprimate","Losartan","C09CA01","Tablet",True),
    ("Metformina Sandoz 1000mg comprimate","Metformin","A10BA02","Tablet",True),
    ("Glimepirid Sandoz 2mg comprimate","Glimepiride","A10BB12","Tablet",True),
    ("Amoxicilina Sandoz 500mg capsule","Amoxicillin","J01CA04","Capsule",True),
    ("Augmentin 625mg comprimate","Amoxicillin/Clavulanate","J01CR02","Tablet",True),
    ("Azitromicina Sandoz 500mg comprimate","Azithromycin","J01FA10","Tablet",True),
    ("Ciprofloxacina Sandoz 500mg comprimate","Ciprofloxacin","J01MA02","Tablet",True),
    ("Levotiroxina Sandoz 100mcg comprimate","Levothyroxine","H03AA01","Tablet",True),
    ("Salbutamol Sandoz 100mcg inhalator","Salbutamol","R03AC02","Inhaler",True),
    ("Sertralina Sandoz 50mg comprimate","Sertraline","N06AB06","Tablet",True),
    ("Escitalopram Sandoz 10mg comprimate","Escitalopram","N06AB10","Tablet",True),
    ("Hidrocortizon 1% crema","Hydrocortisone","D07AA02","Cream",False),
    ("Clotrimazol Sandoz 1% crema","Clotrimazole","D01AC01","Cream",False),
    ("Vitamina D3 1000 UI","Cholecalciferol","A11CC05","Capsule",False),
    ("Magne B6 comprimate","Magnesium/Vitamin B6","A12CC","Tablet",False),
    ("Zolpidem Sandoz 10mg comprimate","Zolpidem","N05CF02","Tablet",True),
],
"GR":[
    ("Depon 500mg δισκία","Paracetamol","N02BE01","Tablet",False),
    ("Panadol 500mg δισκία","Paracetamol","N02BE01","Tablet",False),
    ("Nurofen 200mg δισκία","Ibuprofen","M01AE01","Tablet",False),
    ("Brufen 400mg δισκία","Ibuprofen","M01AE01","Tablet",False),
    ("Aspirin 500mg δισκία","Aspirin","N02BA01","Tablet",False),
    ("Voltaren Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Zyrtec 10mg δισκία","Cetirizine","R06AE07","Tablet",False),
    ("Aerius 5mg δισκία","Desloratadine","R06AX27","Tablet",True),
    ("Otrivin 0.1% ρινικό σπρέι","Xylometazoline","R01AA07","Nasal spray",False),
    ("ACC 200mg αναβράζοντα δισκία","Acetylcysteine","R05CB01","Effervescent tablet",False),
    ("Imodium 2mg κάψουλες","Loperamide","A07DA03","Capsule",False),
    ("Omeprazol 20mg κάψουλες","Omeprazole","A02BC01","Capsule",True),
    ("Pantoprazol 40mg δισκία","Pantoprazole","A02BC02","Tablet",True),
    ("Atorvastatin 20mg δισκία","Atorvastatin","C10AA05","Tablet",True),
    ("Simvastatin 20mg δισκία","Simvastatin","C10AA01","Tablet",True),
    ("Amlodipine 5mg δισκία","Amlodipine","C08CA01","Tablet",True),
    ("Bisoprolol 5mg δισκία","Bisoprolol","C07AB07","Tablet",True),
    ("Ramipril 5mg δισκία","Ramipril","C09AA05","Tablet",True),
    ("Losartan 50mg δισκία","Losartan","C09CA01","Tablet",True),
    ("Metformin 1000mg δισκία","Metformin","A10BA02","Tablet",True),
    ("Amoxicillin 500mg κάψουλες","Amoxicillin","J01CA04","Capsule",True),
    ("Augmentin 625mg δισκία","Amoxicillin/Clavulanate","J01CR02","Tablet",True),
    ("Azithromycin 500mg δισκία","Azithromycin","J01FA10","Tablet",True),
    ("Ciprofloxacin 500mg δισκία","Ciprofloxacin","J01MA02","Tablet",True),
    ("Levothyroxin 100mcg δισκία","Levothyroxine","H03AA01","Tablet",True),
    ("Salbutamol 100mcg εισπνευστήρας","Salbutamol","R03AC02","Inhaler",True),
    ("Sertraline 50mg δισκία","Sertraline","N06AB06","Tablet",True),
    ("Escitalopram 10mg δισκία","Escitalopram","N06AB10","Tablet",True),
    ("Hydrocortisone 1% κρέμα","Hydrocortisone","D07AA02","Cream",False),
    ("Clotrimazole 1% κρέμα","Clotrimazole","D01AC01","Cream",False),
    ("Vitamin D3 1000 IU","Cholecalciferol","A11CC05","Capsule",False),
    ("Zolpidem 10mg δισκία","Zolpidem","N05CF02","Tablet",True),
],
"LU":[
    ("Doliprane Comprimé 500mg","Paracetamol","N02BE01","Tablet",False),
    ("Doliprane Comprimé 1000mg","Paracetamol","N02BE01","Tablet",False),
    ("Doliprane Suspension 2.4%","Paracetamol","N02BE01","Suspension",False),
    ("Efferalgan Comprimé 500mg","Paracetamol","N02BE01","Effervescent tablet",False),
    ("Efferalgan Comprimé 1000mg","Paracetamol","N02BE01","Effervescent tablet",False),
    ("Dafalgan Comprimé 500mg","Paracetamol","N02BE01","Tablet",False),
    ("Dafalgan Comprimé 1000mg","Paracetamol","N02BE01","Tablet",False),
    ("Dafalgan Sirop","Paracetamol","N02BE01","Syrup",False),
    ("Aspirine UPSA 500mg","Aspirin","N02BA01","Effervescent tablet",False),
    ("Ben-u-ron Tablette 500mg","Paracetamol","N02BE01","Tablet",False),
    ("Thomapyrin Tablette","Paracetamol+ASS+Coffein","N02BA51","Tablet",False),
    ("Nurofen Comprimé 200mg","Ibuprofen","M01AE01","Tablet",False),
    ("Nurofen Comprimé 400mg","Ibuprofen","M01AE01","Tablet",False),
    ("Ratiopharm Ibuprofen 400mg","Ibuprofen","M01AE01","Tablet",False),
    ("Voltarène Emulgel 1%","Diclofenac","M02AA15","Gel",False),
    ("Voltarène Emulgel 2%","Diclofenac","M02AA15","Gel",False),
    ("Otrivine Spray nasal 0.1%","Xylometazoline","R01AA07","Nasal spray",False),
    ("Rhinathiol Sirop 2%","Carbocisteine","R05CB03","Syrup",False),
    ("Bisolvon Sirop","Bromhexine","R05CB02","Syrup",False),
    ("Zyrtec Comprimé 10mg","Cetirizine","R06AE07","Tablet",False),
    ("Clarityne Comprimé 10mg","Loratadine","R06AX13","Tablet",False),
    ("Aerius Comprimé 5mg","Desloratadine","R06AX27","Tablet",True),
    ("Mopral Gélule 20mg","Omeprazole","A02BC01","Capsule",True),
    ("Nexium Control Comprimé 20mg","Esomeprazole","A02BC05","Tablet",False),
    ("Imodium Gélule 2mg","Loperamide","A07DA03","Capsule",False),
    ("Smecta 3g Poudre","Diosmectite","A07BC05","Powder",False),
    ("Bisoprolol Ratiopharm 5mg","Bisoprolol","C07AB07","Tablet",True),
    ("Amlodipine Ratiopharm 5mg","Amlodipine","C08CA01","Tablet",True),
    ("Ramipril Ratiopharm 5mg","Ramipril","C09AA05","Tablet",True),
    ("Losartan Ratiopharm 50mg","Losartan","C09CA01","Tablet",True),
    ("Atorvastatin Ratiopharm 20mg","Atorvastatin","C10AA05","Tablet",True),
    ("Metformin Ratiopharm 1000mg","Metformin","A10BA02","Tablet",True),
    ("Sertraline Ratiopharm 50mg","Sertraline","N06AB06","Tablet",True),
    ("Amoxicilline Ratiopharm 500mg","Amoxicillin","J01CA04","Capsule",True),
    ("Azithromycine Ratiopharm 500mg","Azithromycin","J01FA10","Tablet",True),
    ("Levothyroxine Ratiopharm 100mcg","Levothyroxine","H03AA01","Tablet",True),
    ("Salbutamol Ratiopharm 100mcg","Salbutamol","R03AC02","Inhaler",True),
    ("Vitamin D3 1000 UI","Cholecalciferol","A11CC05","Capsule",False),
    ("Calcium + Vitamin D3 Sandoz","Calcium/Vitamin D","A12AX","Tablet",False),
    ("Hydrocortisone 1% Crème","Hydrocortisone","D07AA02","Cream",False),
    ("Clotrimazole 1% Crème","Clotrimazole","D01AC01","Cream",False),
],
}

# ── MAIN ─────────────────────────────────────────────────────────
print(f"\n🌍 {COUNTRY} — medicijnen ophalen...")
seen = set()
rows = []

# 1. EMA
rows.extend(fetch_ema(seen))

# 2. Nationaal
nat = NATIONAL[COUNTRY](seen)
if nat:
    print(f"  ✅ Nationaal: {len(nat)} records")
    rows.extend(nat)
else:
    print(f"  ⚠️  Nationaal niet beschikbaar — alleen EMA + fallback")

# 3. Fallback (nooit leeg)
added = 0
for name, inn, atc, form, rx in FALLBACK.get(COUNTRY, []):
    key = name.lower()
    if key not in seen:
        seen.add(key)
        rows.append({"name":name,"generic":inn,"atc":atc,"pharmaceutical_form":form,
                     "rx":"Rx" if rx else "","status":"authorised"})
        added += 1
if added:
    print(f"  📋 Fallback: {added} records")

print(f"\n  📊 Totaal: {len(rows)} unieke records")

with open(OUTPUT,"w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["name","generic","atc","pharmaceutical_form","rx","status"])
    w.writeheader(); w.writerows(rows)

print(f"✅ {COUNTRY}: {len(rows)} medicijnen → {OUTPUT} ({os.path.getsize(OUTPUT)//1024} KB)")
