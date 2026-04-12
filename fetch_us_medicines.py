#!/usr/bin/env python3
"""
apoHouze — Verenigde Staten Medicijnen Fetcher v2
==================================================
Bron: FDA NDC Directory flat files (fda.gov/drugs)
  https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory

Het FDA NDC product.txt bevat 100.000+ producten met:
  PROPRIETARYNAME  → merknaam
  NONPROPRIETARYNAME → generieke naam
  PHARMACLASSCS    → farmacologische klasse (voor categoriemapping)
  DOSAGEFORMNAME   → farmaceutische vorm
  DEASCHEDULE      → DEA-schedule (voor Rx/OTC)
  MARKETINGCATEGORYNAME → "PRESCRIPTION" / "OTC MONOGRAPH" etc.

Fallback: als fda.gov geblokkeerd is, gebruik openFDA API met
  paginering (max 1000/aanroep, geen API-key nodig) voor alle
  records met pharm_class, en generieke-naam-mapping voor de rest.

Output: data/_tmp/us_medicines.csv
  Kolommen: Name,INN,ATC,PharmaceuticalForm,RxStatus,Country

Gebruik: python3 fetch_us_medicines.py [--debug]
"""

import sys, os, re, csv, time, subprocess, json, zipfile, io, urllib.request

DEBUG = "--debug" in sys.argv
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
TMP_DIR     = os.path.join(SCRIPT_DIR, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "us_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

# FDA NDC flat file ZIP (bevat product.txt met alle producten)
FDA_NDC_URLS = [
    "https://www.accessdata.fda.gov/cder/ndctext.zip",
    "https://www.fda.gov/files/drugs/published/NDC-Database-File---Product-Labeler.zip",
]

# Generieke naam → categorie (breed keyword-gebaseerd, werkt op NONPROPRIETARYNAME)
# Dit is de primaire mapping als PHARMACLASSCS ontbreekt
GENERIC_MAP = [
    # Pain & Fever
    (r"\b(paracetamol|acetaminophen|ibuprofen|naproxen|aspirin|diclofenac|"
     r"celecoxib|meloxicam|ketoprofen|piroxicam|indomethacin|ketorolac|"
     r"tramadol|codeine|oxycodone|hydrocodone|morphine|fentanyl|buprenorphine|"
     r"methadone|hydromorphone|oxymorphone|tapentadol)\b", "Pain & Fever"),
    # Antibiotics
    (r"\b(amoxicillin|amoxicilline|ampicillin|penicillin|azithromycin|"
     r"clarithromycin|erythromycin|doxycycline|minocycline|tetracycline|"
     r"ciprofloxacin|levofloxacin|moxifloxacin|ofloxacin|trimethoprim|"
     r"sulfamethoxazole|metronidazole|clindamycin|vancomycin|linezolid|"
     r"cephalexin|cefuroxime|ceftriaxone|cefdinir|nitrofurantoin|fosfomycin|"
     r"rifampin|isoniazid|ethambutol|pyrazinamide)\b", "Antibiotics"),
    # Antivirals
    (r"\b(acyclovir|valacyclovir|famciclovir|oseltamivir|zanamivir|"
     r"tenofovir|emtricitabine|efavirenz|lopinavir|ritonavir|atazanavir|"
     r"dolutegravir|bictegravir|sofosbuvir|ledipasvir|ribavirin|ganciclovir|"
     r"valganciclovir|cidofovir|nirmatrelvir|molnupiravir)\b", "Antivirals"),
    # Antifungals
    (r"\b(fluconazole|itraconazole|voriconazole|posaconazole|ketoconazole|"
     r"clotrimazole|miconazole|terbinafine|nystatin|amphotericin|"
     r"griseofulvin|econazole|butoconazole)\b", "Antifungals"),
    # Antiparasitics
    (r"\b(metronidazole|tinidazole|ivermectin|mebendazole|albendazole|"
     r"praziquantel|pyrantel|hydroxychloroquine|chloroquine|atovaquone|"
     r"permethrin|lindane|spinosad)\b", "Antiparasitics"),
    # Allergy
    (r"\b(loratadine|cetirizine|fexofenadine|levocetirizine|desloratadine|"
     r"diphenhydramine|chlorpheniramine|hydroxyzine|azelastine|olopatadine|"
     r"brompheniramine|clemastine|promethazine)\b", "Allergy"),
    # Cough & Cold
    (r"\b(dextromethorphan|guaifenesin|pseudoephedrine|phenylephrine|"
     r"xylometazoline|oxymetazoline|naphazoline|ipratropium|benzonatate|"
     r"bromhexine|ambroxol|acetylcysteine)\b", "Cough & Cold"),
    # Lungs & Asthma
    (r"\b(albuterol|salbutamol|levalbuterol|salmeterol|formoterol|"
     r"tiotropium|ipratropium|budesonide|fluticasone|beclomethasone|"
     r"mometasone|ciclesonide|montelukast|zafirlukast|theophylline|"
     r"roflumilast|omalizumab|dupilumab|benralizumab)\b", "Lungs & Asthma"),
    # Stomach & Intestine
    (r"\b(omeprazole|pantoprazole|esomeprazole|lansoprazole|rabeprazole|"
     r"ranitidine|famotidine|cimetidine|antacid|simethicone|loperamide|"
     r"bismuth|metoclopramide|ondansetron|prochlorperazine|domperidone|"
     r"docusate|bisacodyl|senna|lactulose|polyethylene glycol|macrogol|"
     r"mesalazine|mesalamine|sulfasalazine|infliximab|vedolizumab)\b", "Stomach & Intestine"),
    # Heart & Blood Pressure
    (r"\b(amlodipine|lisinopril|losartan|valsartan|atenolol|metoprolol|"
     r"carvedilol|bisoprolol|enalapril|ramipril|captopril|irbesartan|"
     r"candesartan|olmesartan|telmisartan|hydrochlorothiazide|furosemide|"
     r"spironolactone|digoxin|amiodarone|sotalol|diltiazem|verapamil|"
     r"nifedipine|felodipine|hydralazine|minoxidil|clonidine|doxazosin|"
     r"prazosin|sacubitril|ivabradine|eplerenone)\b", "Heart & Blood Pressure"),
    # Cholesterol
    (r"\b(atorvastatin|simvastatin|rosuvastatin|pravastatin|lovastatin|"
     r"fluvastatin|pitavastatin|ezetimibe|fenofibrate|gemfibrozil|niacin|"
     r"evolocumab|alirocumab|inclisiran|bempedoic)\b", "Cholesterol"),
    # Anticoagulants
    (r"\b(warfarin|heparin|enoxaparin|apixaban|rivaroxaban|dabigatran|"
     r"edoxaban|clopidogrel|ticagrelor|prasugrel|aspirin.*\b81mg\b|"
     r"dipyridamole|fondaparinux|argatroban|bivalirudin)\b", "Anticoagulants"),
    # Diabetes
    (r"\b(metformin|glipizide|glyburide|glimepiride|pioglitazone|"
     r"rosiglitazone|sitagliptin|saxagliptin|linagliptin|alogliptin|"
     r"empagliflozin|canagliflozin|dapagliflozin|liraglutide|semaglutide|"
     r"exenatide|dulaglutide|insulin|acarbose|miglitol|repaglinide|"
     r"nateglinide|tirzepatide)\b", "Diabetes"),
    # Thyroid
    (r"\b(levothyroxine|liothyronine|methimazole|propylthiouracil|"
     r"thyroid|potassium iodide)\b", "Thyroid"),
    # Corticosteroids
    (r"\b(prednisone|prednisolone|methylprednisolone|dexamethasone|"
     r"hydrocortisone|betamethasone|triamcinolone|fludrocortisone)\b", "Corticosteroids"),
    # Neurology
    (r"\b(levodopa|carbidopa|ropinirole|pramipexole|rasagiline|selegiline|"
     r"donepezil|rivastigmine|galantamine|memantine|gabapentin|pregabalin|"
     r"phenytoin|valproate|valproic acid|carbamazepine|oxcarbazepine|"
     r"lamotrigine|topiramate|levetiracetam|zonisamide|lacosamide|"
     r"eslicarbazepine|brivaracetam|perampanel|cenobamate)\b", "Neurology"),
    # Sleep & Sedation
    (r"\b(zolpidem|zaleplon|eszopiclone|temazepam|triazolam|flurazepam|"
     r"quazepam|diazepam|lorazepam|alprazolam|clonazepam|midazolam|"
     r"buspirone|melatonin|ramelteon|suvorexant|lemborexant|doxepin)\b", "Sleep & Sedation"),
    # Antidepressants
    (r"\b(sertraline|fluoxetine|paroxetine|escitalopram|citalopram|"
     r"venlafaxine|duloxetine|desvenlafaxine|levomilnacipran|bupropion|"
     r"mirtazapine|amitriptyline|nortriptyline|imipramine|clomipramine|"
     r"phenelzine|tranylcypromine|selegiline|trazodone|vilazodone|"
     r"vortioxetine|fluvoxamine|lithium|aripiprazole|quetiapine.*depress)\b", "Antidepressants"),
    # Vitamins & Supplements
    (r"\b(vitamin [abcdedk]|thiamine|riboflavin|niacin|folic acid|"
     r"cyanocobalamin|ascorbic acid|cholecalciferol|ergocalciferol|"
     r"tocopherol|phytonadione|ferrous|iron|calcium|zinc|magnesium|"
     r"potassium|sodium|electrolyte|multivitamin)\b", "Vitamins & Supplements"),
    # Women's Health
    (r"\b(ethinyl estradiol|estradiol|conjugated estrogen|medroxyprogesterone|"
     r"levonorgestrel|norethindrone|desogestrel|drospirenone|etonogestrel|"
     r"progesterone|clomiphene|letrozole|misoprostol|dinoprostone|oxytocin|"
     r"mifepristone|ulipristal)\b", "Women's Health"),
    # Urology
    (r"\b(tamsulosin|alfuzosin|silodosin|doxazosin.*prostate|finasteride|"
     r"dutasteride|oxybutynin|tolterodine|solifenacin|darifenacin|"
     r"mirabegron|sildenafil|tadalafil|vardenafil|avanafil)\b", "Urology"),
    # Oncology
    (r"\b(tamoxifen|letrozole|anastrozole|exemestane|fulvestrant|"
     r"imatinib|erlotinib|gefitinib|osimertinib|dasatinib|nilotinib|"
     r"ibrutinib|venetoclax|bortezomib|lenalidomide|thalidomide|"
     r"cyclophosphamide|methotrexate.*oncol|capecitabine|temozolomide|"
     r"bevacizumab|rituximab|trastuzumab|pembrolizumab|nivolumab)\b", "Oncology"),
    # Joints & Muscles
    (r"\b(methotrexate|hydroxychloroquine|sulfasalazine|leflunomide|"
     r"etanercept|adalimumab|tocilizumab|abatacept|baricitinib|"
     r"tofacitinib|upadacitinib|colchicine|allopurinol|febuxostat|"
     r"probenecid|cyclobenzaprine|methocarbamol|baclofen|tizanidine|"
     r"carisoprodol|alendronate|risedronate|zoledronic|denosumab)\b", "Joints & Muscles"),
    # Skin & Wounds
    (r"\b(tretinoin|adapalene|tazarotene|benzoyl peroxide|clindamycin.*topical|"
     r"erythromycin.*topical|dapsone|isotretinoin|acitretin|"
     r"tacrolimus.*topical|pimecrolimus|clobetasol|halobetasol|"
     r"calcipotriene|coal tar|salicylic acid|urea.*topical|minoxidil.*topical)\b", "Skin & Wounds"),
    # Eye & Ear
    (r"\b(latanoprost|timolol.*eye|bimatoprost|travoprost|dorzolamide|"
     r"brimonidine|pilocarpine|ciprofloxacin.*eye|ofloxacin.*eye|"
     r"tobramycin.*eye|gentamicin.*eye|neomycin.*ear|hydrocortisone.*ear|"
     r"prednisolone.*eye|dexamethasone.*eye|fluorometholone|"
     r"cyclopentolate|atropine.*eye|artificial tear)\b", "Eye & Ear"),
    # First Aid
    (r"\b(lidocaine|benzocaine|procaine|bupivacaine|ropivacaine|"
     r"chlorhexidine|povidone.iodine|hydrogen peroxide|bacitracin|"
     r"neomycin|polymyxin|silver sulfadiazine|mupirocin)\b", "First Aid"),
]

BLACKLIST = re.compile(
    r"\b(vaccine|vaccin|immunoglobulin|plasma|albumin|diagnostic|"
     r"dressing|device|reagent|contrast media|radioactive|"
     r"blood product|hemodialysis|peritoneal)\b", re.I
)


def name_to_category(name):
    """Generieke naam → categorie via brede keyword-matching."""
    if not name:
        return None
    nl = name.lower()
    for pattern, cat in GENERIC_MAP:
        if re.search(pattern, nl, re.I):
            return cat
    return None


def pharm_class_to_category(pharm_classes):
    """
    FDA PHARMACLASSCS string → categorie.
    Bevat strings als "Proton Pump Inhibitor [EPC]",
    "Nonsteroidal Anti-inflammatory Drug [EPC]", etc.
    """
    if not pharm_classes:
        return None
    text = " ".join(pharm_classes).lower() if isinstance(pharm_classes, list) else str(pharm_classes).lower()
    map_pharm = [
        (r"analgesic|pain|antipyretic|nonsteroidal anti.inflam|opioid|narcotic", "Pain & Fever"),
        (r"antibacterial|antibiotic|penicillin|cephalosporin|macrolide|quinolone|tetracycline", "Antibiotics"),
        (r"antiviral|antiretroviral|neuraminidase|reverse transcriptase", "Antivirals"),
        (r"antifungal|azole|polyene", "Antifungals"),
        (r"antiparasit|anthelmintic|antimalarial|antiprotozoal", "Antiparasitics"),
        (r"antihistamine|histamine.*receptor.*antagonist", "Allergy"),
        (r"decongestant|expectorant|antitussive|mucolytic", "Cough & Cold"),
        (r"bronchodilator|beta.*agonist|anticholinergic.*pulmonary|leukotriene|corticosteroid.*pulmonary", "Lungs & Asthma"),
        (r"proton pump|antacid|h2.*receptor|laxative|antidiarrheal|antiemetic|gastrointestinal motility", "Stomach & Intestine"),
        (r"antihypertensive|beta.*blocker|ace.*inhibitor|angiotensin|calcium.*channel.*blocker|diuretic|vasodilator|cardiac glycoside", "Heart & Blood Pressure"),
        (r"statin|hmg.coa|lipid.lowering|cholesterol", "Cholesterol"),
        (r"anticoagulant|antiplatelet|thrombolytic|factor xa|thrombin inhibitor", "Anticoagulants"),
        (r"antidiabetic|insulin|hypoglycemic|glp.1|sglt|dpp.4|incretin", "Diabetes"),
        (r"thyroid|antithyroid", "Thyroid"),
        (r"corticosteroid|glucocorticoid|mineralocorticoid", "Corticosteroids"),
        (r"anticonvulsant|antiepileptic|anti.parkinson|dopamine.*agonist|cholinesterase", "Neurology"),
        (r"sedative|hypnotic|anxiolytic|benzodiazepine|gaba.*agonist", "Sleep & Sedation"),
        (r"antidepressant|ssri|snri|serotonin.*reuptake|monoamine", "Antidepressants"),
        (r"vitamin|mineral|supplement|iron|folic|electrolyte", "Vitamins & Supplements"),
        (r"contraceptive|estrogen|progestin|hormone.*replacement", "Women's Health"),
        (r"alpha.*blocker.*urol|benign.*prostate|phosphodiesterase type 5", "Urology"),
        (r"antineoplastic|chemotherapy|cytotoxic|kinase inhibitor|checkpoint inhibitor", "Oncology"),
        (r"muscle.*relaxant|antigout|uricosuric|bisphosphonate|dmard", "Joints & Muscles"),
        (r"topical.*dermatologic|retinoid|keratolytic", "Skin & Wounds"),
        (r"ophthalmic|ocular|otic|glaucoma", "Eye & Ear"),
        (r"local anesthetic|antiseptic|topical anti-infective", "First Aid"),
    ]
    for pattern, cat in map_pharm:
        if re.search(pattern, text, re.I):
            return cat
    return None


def curl_download(url, dest, max_time=300):
    cmd = [
        "curl", "-L", "--max-time", str(max_time), "--connect-timeout", "20",
        "--silent", "--fail",
        "--user-agent", "Mozilla/5.0 apoHouze-updater/5.0",
        "-o", dest, url,
    ]
    for attempt in range(3):
        try:
            subprocess.run(cmd, timeout=max_time + 15, check=True)
            size = os.path.getsize(dest)
            print(f"  ✅ {size // 1024} KB gedownload")
            return size
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"  ⚠️  Poging {attempt+1}/3: {e}")
            if attempt < 2: time.sleep(5)
    return 0


def process_ndc_product_txt(zip_path):
    """
    Verwerk de FDA NDC product.txt uit ndctext.zip.

    Kolommen (tab-gescheiden, met header):
      PRODUCTID, PRODUCTNDC, PRODUCTTYPENAME, PROPRIETARYNAME,
      PROPRIETARYNAMESUFFIX, NONPROPRIETARYNAME, DOSAGEFORMNAME,
      ROUTENAME, STARTMARKETINGDATE, ENDMARKETINGDATE,
      MARKETINGCATEGORYNAME, APPLICATIONNUMBER, LABELERNAME,
      SUBSTANCENAME, ACTIVE_NUMERATOR_STRENGTH, ACTIVE_INGRED_UNIT,
      PHARM_CLASSES, DEASCHEDULE, NDC_EXCLUDE_FLAG, LISTING_RECORD_CERTIFIED_THROUGH
    """
    print(f"  📦 ZIP openen ({os.path.getsize(zip_path)//1024} KB)...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if DEBUG: print(f"  🔍 ZIP inhoud: {names}")
        # Zoek product.txt (of Products.txt)
        prod_name = next((n for n in names if "product" in n.lower() and n.lower().endswith(".txt")), None)
        if not prod_name:
            raise RuntimeError(f"product.txt niet gevonden in ZIP. Bestanden: {names}")
        print(f"  📖 {prod_name} lezen...")
        with zf.open(prod_name) as f:
            content = f.read().decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    rows = list(reader)
    print(f"  📊 {len(rows)} rijen | Kolommen: {list(rows[0].keys())[:8] if rows else '?'}")

    results = []
    seen = set()
    sk_bl = 0; sk_cat = 0; sk_end = 0

    for row in rows:
        # Sla vervallen producten over
        end_date = row.get("ENDMARKETINGDATE", "").strip()
        if end_date and len(end_date) == 8:
            # YYYYMMDD formaat
            try:
                import datetime
                ed = datetime.datetime.strptime(end_date, "%Y%m%d")
                if ed < datetime.datetime.now():
                    sk_end += 1; continue
            except ValueError:
                pass

        brand   = row.get("PROPRIETARYNAME", "").strip()
        generic = row.get("NONPROPRIETARYNAME", "").strip()
        name    = brand or generic
        if not name: continue

        if BLACKLIST.search(name) or BLACKLIST.search(generic):
            sk_bl += 1; continue

        # Probeer categorie: eerst pharm_class, dan generieke naam
        pharm = row.get("PHARM_CLASSES", "").strip()
        pharm_list = [p.strip() for p in pharm.split(",")] if pharm else []
        category = pharm_class_to_category(pharm_list) or name_to_category(generic) or name_to_category(name)
        if not category:
            sk_cat += 1; continue

        # Rx/OTC
        mkt = row.get("MARKETINGCATEGORYNAME", "").upper()
        dea = row.get("DEASCHEDULE", "").strip()
        rx  = bool(dea) or "OTC" not in mkt

        form = row.get("DOSAGEFORMNAME", "").strip()

        key = name.lower()
        if key in seen: continue
        seen.add(key)

        results.append({
            "Name": name, "INN": generic, "ATC": "",
            "PharmaceuticalForm": form,
            "RxStatus": "Rx" if rx else "OTC",
            "Country": "US",
        })

    print(f"  ✅ {len(results)} uniek | {sk_cat} geen categorie | {sk_bl} blacklist | {sk_end} vervallen")
    return results


def save_csv(rows):
    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"\n✅ {len(rows)} medicijnen opgeslagen → {OUTPUT_FILE}")


def main():
    print("🇺🇸 apoHouze — Verenigde Staten Medicijnen Fetcher v2")
    print("=" * 54)
    print("📌 Bron: FDA NDC Directory flat files (ndctext.zip)\n")

    dest = os.path.join(TMP_DIR, "us_ndctext.zip")

    print("[1/3] FDA NDC flat files downloaden...")
    downloaded = False
    for url in FDA_NDC_URLS:
        print(f"  📥 {url}")
        size = curl_download(url, dest)
        if size > 100_000:
            downloaded = True
            break
        print(f"  ⚠️  Te klein ({size}B), volgende proberen...")

    if not downloaded:
        print("❌ FDA NDC download mislukt — alle URLs geprobeerd")
        sys.exit(1)

    print(f"\n[2/3] product.txt verwerken...")
    try:
        results = process_ndc_product_txt(dest)
    except Exception as e:
        print(f"❌ Verwerking mislukt: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    if not results:
        print("❌ Geen geldige medicijnen gevonden"); sys.exit(1)

    print(f"\n[3/3] Opslaan ({len(results)} unieke producten)...")
    save_csv(results)


if __name__ == "__main__":
    main()
