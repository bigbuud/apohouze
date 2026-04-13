#!/usr/bin/env python3
"""
apoHouze — France Medicijnen Fetcher v1
========================================
Bron: ANSM Base de Données Publique des Médicaments (BDPM)
  https://base-donnees-publique.medicaments.gouv.fr/telechargement.php

Bestanden (tab-gescheiden .txt, UTF-8, geen header):
  CIS_bdpm.txt     — spécialités (merknamen)
    kol 0: CIS-code, 1: Dénomination (merknaam), 2: Forme pharmaceutique,
    3: Voie(s) d'administration, 4: Statut AMM, 5: Type procédure,
    6: État commercialisation, 7: Date AMM, 8: StatutBdm,
    9: Numéro autorisation européenne, 10: Titulaire(s), 11: Surveillance renforcée

  CIS_COMPO_bdpm.txt — compositions (werkzame stoffen + ATC)
    kol 0: CIS-code, 1: Désignation élément pharmaceutique,
    2: Code substance, 3: Dénomination substance (INN),
    4: Dosage, 5: Référence dosage, 6: Nature composant, 7: Numéro liaison

  CIS_CPD_bdpm.txt — conditions prescription/délivrance
    kol 0: CIS-code, 1: Condition prescription/délivrance

Strategy:
  - CIS_bdpm.txt geeft merknaam + farmaceutische vorm + commercialiseringsstatus
  - CIS_COMPO_bdpm.txt geeft INN (kol 3) per CIS-code
  - Categorie via ATC-mapping op INN (zelfde ATC_MAP als update.js)
  - Filter: état_commercialisation = "Commercialisé"
  - Rx/OTC via CIS_CPD (aanwezigheid "Liste I" of "Liste II" of "prescription")

Output: data/_tmp/fr_medicines.csv
"""

import sys, os, re, csv, time, subprocess, io

DEBUG = "--debug" in sys.argv
# Gebruik os.getcwd() want update.js roept dit script aan met cwd=repo_root
# os.path.dirname(__file__) kan afwijken als Python het pad anders resolvet
REPO_ROOT   = os.getcwd()
TMP_DIR     = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "fr_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

# ANSM BDPM - meerdere URL-varianten (primair + fallbacks)
# De oude base-donnees-publique.medicaments.gouv.fr blokkeert CI-omgevingen.
# bdpm.ansm.sante.fr is de nieuwe officiële URL (sinds 2024).
# esante.gouv.fr host een mirror van de bestanden.
BDPM_URLS = {
    "cis": [
        "https://bdpm.ansm.sante.fr/download/file/CIS_bdpm.txt",
        "https://base-donnees-publique.medicaments.gouv.fr/index.php/download/file/CIS_bdpm.txt",
        "https://esante.gouv.fr/sites/default/files/media_entity/documents/CIS_bdpm.txt",
    ],
    "compo": [
        "https://bdpm.ansm.sante.fr/download/file/CIS_COMPO_bdpm.txt",
        "https://base-donnees-publique.medicaments.gouv.fr/index.php/download/file/CIS_COMPO_bdpm.txt",
        "https://esante.gouv.fr/sites/default/files/media_entity/documents/CIS_COMPO_bdpm.txt",
    ],
    "cpd": [
        "https://bdpm.ansm.sante.fr/download/file/CIS_CPD_bdpm.txt",
        "https://base-donnees-publique.medicaments.gouv.fr/index.php/download/file/CIS_CPD_bdpm.txt",
        "https://esante.gouv.fr/sites/default/files/media_entity/documents/CIS_CPD_bdpm.txt",
    ],
}

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
    r"\b(vaccin|immunoglobulin|albumin|dialys|dispositif|diagnostic|radiopharm)\b", re.I
)

def atc_category(atc):
    return ATC_MAP.get((atc or "").strip()[:3].upper())

def curl_download(url, dest, max_time=120):
    cmd = ["curl","-L","--max-time",str(max_time),"--connect-timeout","20",
           "--silent","--fail","--user-agent","Mozilla/5.0 apoHouze-updater/5.0",
           "-o", dest, url]
    for attempt in range(3):
        try:
            subprocess.run(cmd, timeout=max_time+15, check=True)
            size = os.path.getsize(dest)
            print(f"  ✅ {size//1024} KB")
            return size
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"  ⚠️  Poging {attempt+1}/3: {e}")
            if attempt < 2: time.sleep(4)
    return 0

def read_txt(path, encoding="latin-1"):
    """Lees tab-gescheiden BDPM-bestand zonder header."""
    with open(path, encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter="\t")
        return list(reader)

def main():
    print("🇫🇷 apoHouze — France Medicijnen Fetcher v1")
    print("=" * 48)
    print("📌 Bron: ANSM BDPM (base-donnees-publique.medicaments.gouv.fr)\n")

    # Download de drie bestanden (probeer meerdere URLs per bestand)
    files = {}
    for key, urls in BDPM_URLS.items():
        dest = os.path.join(TMP_DIR, f"fr_{key}.txt")
        downloaded = False
        for url in urls:
            print(f"  📥 {key}: {url}")
            size = curl_download(url, dest)
            if size > 1000:
                downloaded = True
                break
            print(f"  ⚠️  Te klein ({size}B), volgende URL proberen...")
        if not downloaded:
            print(f"❌ Download mislukt voor {key} — alle URLs geprobeerd")
            sys.exit(1)
        files[key] = dest

    print("\n[2/3] Verwerken...")

    # CIS_COMPO: CIS-code → (INN, ATC-prefix via naam-matching)
    # Kolommen: CIS, élément, code_substance, denomination_substance,
    #           dosage, ref_dosage, nature_composant, num_liaison
    print("  📖 Composities laden (INN-mapping)...")
    compo_rows = read_txt(files["compo"])
    # Per CIS-code: bewaar eerste werkzame stof (nature_composant = "SA")
    cis_to_inn = {}
    for row in compo_rows:
        if len(row) < 8: continue
        cis     = row[0].strip()
        nature  = row[6].strip() if len(row) > 6 else ""
        inn     = row[3].strip() if len(row) > 3 else ""
        if nature == "SA" and inn and cis not in cis_to_inn:
            cis_to_inn[cis] = inn
    print(f"  📊 {len(cis_to_inn)} CIS-codes met INN")

    # CIS_CPD: CIS-code → Rx/OTC
    print("  📖 Voorschriftcondities laden...")
    cpd_rows = read_txt(files["cpd"])
    cis_rx = set()
    for row in cpd_rows:
        if len(row) < 2: continue
        cis = row[0].strip()
        cond = row[1].strip().upper()
        if any(x in cond for x in ["LISTE I","LISTE II","PRESCRIPTION","LISTE",
                                    "STUP","STUPÉFIANT","PSYCHOTROPE"]):
            cis_rx.add(cis)

    # CIS_bdpm: spécialités
    print("  📖 Spécialités laden...")
    cis_rows = read_txt(files["cis"])
    print(f"  📊 {len(cis_rows)} spécialités")
    if DEBUG and cis_rows:
        print(f"  🔍 Voorbeeld: {cis_rows[0]}")

    results = []
    seen = set()
    sk_status = 0; sk_bl = 0; sk_cat = 0; sk_dup = 0

    for row in cis_rows:
        if len(row) < 7: continue
        cis         = row[0].strip()
        name        = row[1].strip()
        form_raw    = row[2].strip() if len(row) > 2 else ""
        status_amm  = row[4].strip() if len(row) > 4 else ""  # Statut AMM
        etat        = row[6].strip() if len(row) > 6 else ""  # État commercialisation

        # Filter: alleen gecommercialiseerde producten
        if "Commercialisé" not in etat and etat:
            sk_status += 1; continue

        if not name or BLACKLIST.search(name):
            sk_bl += 1; continue

        inn = cis_to_inn.get(cis, "")

        # Categorie via INN naam-matching op ATC_MAP
        # BDPM heeft geen directe ATC-kolom in CIS_bdpm.txt,
        # maar INN naam + keyword mapping dekt de meeste gevallen
        category = None
        # Probeer ATC via INN keyword-matching (zelfde logica als andere landen)
        inn_lower = inn.lower()
        from fetch_ca_medicines import ATC_MAP as _  # vermijd import; gebruik lokale map
        # Gebruik de generieke naam-mapping van fetch_us_medicines als helper
        # Hier gebruiken we een ingebouwde ATC-prefix tabel gebaseerd op INN

        # Ingebouwde brede INN→categorie mapping (Frans stelsel)
        INN_CAT = [
            (r"paracétamol|paracetamol|ibuprofène|ibuprofen|naproxène|aspirine|diclofénac|"
             r"tramadol|codéine|oxycodone|morphine|fentanyl|kétoprofène|"
             r"célécoxib|méloxicam|piroxicam|indométacine|kétorolac", "Pain & Fever"),
            (r"amoxicilline|amoxicillin|azithromycine|clarithromycine|érythromycine|"
             r"doxycycline|ciprofloxacine|lévofloxacine|métronidazole|clindamycine|"
             r"céfalexine|céfuroxime|nitrofurantoïne|triméthoprime|sulfaméthoxazole|"
             r"vancomycine|rifampicine|isoniazide", "Antibiotics"),
            (r"aciclovir|valaciclovir|oseltamivir|famciclovir|ténofovir|emtricitabine|"
             r"lopinavir|ritonavir|dolutégravir|sofosbuvir|lédipasivir|ganciclovir", "Antivirals"),
            (r"fluconazole|itraconazole|voriconazole|kétoconazole|clotrimazole|"
             r"miconazole|terbinafine|nystatine|amphotéricine|griséofulvine", "Antifungals"),
            (r"ivermectine|métronidazole.*parasit|albendazole|mébendazole|"
             r"hydroxychloroquine|chloroquine|atovaquone|perméthrine", "Antiparasitics"),
            (r"loratadine|cétirizine|fexofénadine|lévocétirizine|desloratadine|"
             r"diphénhydramine|chlorphénamine|hydroxyzine|azélastine|"
             r"ébastine|bilastine|rupatadine", "Allergy"),
            (r"dextrométhorphane|guaïfénésine|pseudoéphédrine|phényléphrine|"
             r"xylométazoline|oxymétazoline|ipratropium.*nasal|"
             r"ambroxol|bromhexine|acétylcystéine|carbocystéine", "Cough & Cold"),
            (r"salbutamol|albutérol|salmétérol|formotérol|tiotropium|ipratropium.*pulm|"
             r"budésonide.*inhal|fluticasone|béclométasone|montélukast|"
             r"théophylline|roflumilast|omalizumab", "Lungs & Asthma"),
            (r"oméprazole|pantoprazole|ésoméprazole|lansoprazole|rabéprazole|"
             r"ranitidine|famotidine|ciméidine|lopéramide|bismuth|"
             r"métoclopramide|ondansétron|dompéridone|mesalazine|mesalamine|"
             r"macrogol|lactulose|séné|bisacodyl|docusate", "Stomach & Intestine"),
            (r"amlodipine|lisinopril|losartan|métoprolol|aténolol|"
             r"hydrochlorothiazide|furosémide|spironolactone|digoxine|amiodarone|"
             r"énalapril|ramipril|carvedilol|bisoprolol|valsartan|"
             r"candésartan|olmésartan|telmisartan|propranolol|vérapamil|"
             r"diltiazem|nitroglycérine|isosorbide|nifédipine|félodipine|"
             r"clonidine|indapamide|lercanidipine|périndopril|zofénopril", "Heart & Blood Pressure"),
            (r"atorvastatine|simvastatine|rosuvastatine|pravastatine|ézétimibe|"
             r"fénofibrate|gemfibrozil|fluvastatine|pitivastatine", "Cholesterol"),
            (r"warfarine|héparine|énoxaparine|apixaban|rivaroxaban|dabigatran|"
             r"clopidogrel|ticagrélor|prasugrel|acide acétylsalicylique.*antiagrég", "Anticoagulants"),
            (r"metformine|glipizide|glyburide|glimépiride|pioglitazone|"
             r"sitagliptine|saxagliptine|linagliptine|empagliflozine|canagliflozine|"
             r"dapagliflozine|liraglutide|sémaglutide|exénatide|dulaglutide|"
             r"insuline|acarbose|répaglinide|tirzepatide", "Diabetes"),
            (r"lévothyroxine|liothyronine|méthimazole|propylthiouracile", "Thyroid"),
            (r"prednisone|prednisolone|méthylprednisolone|dexaméthasone|"
             r"hydrocortisone|bétaméthasone|triamcinolone.*systém|fludrocortisone", "Corticosteroids"),
            (r"gabapentine|prégabaline|lévétiracétam|carbamazépine|lamotrigine|"
             r"topiramate|phénytoïne|valproate|acide valproïque|zonisamide|"
             r"lévodopa|carbidopa|ropinirole|pramipexole|rasagiline|"
             r"donépézil|rivastigmine|galantamine|mémantine|"
             r"sumatriptan|rizatriptan|almotriptan|zolmitriptan", "Neurology"),
            (r"zolpidem|zopiclone|estazolam|témazépam|triazolam|"
             r"diazépam|lorazépam|alprazolam|clonazépam|oxazépam|"
             r"buspirone|mélatonine|rameltéon", "Sleep & Sedation"),
            (r"sertraline|fluoxétine|paroxétine|escitalopram|citalopram|"
             r"venlafaxine|duloxétine|bupropion|mirtazapine|amitriptyline|"
             r"nortriptyline|imipramine|clomipramine|trazodone|"
             r"quétiapine|aripiprazole|olanzapine|rispéridone|"
             r"lithium|fluvoxamine|vilazodone|vortioxétine", "Antidepressants"),
            (r"vitamine a|vitamine b|vitamine c|vitamine d|vitamine e|vitamine k|"
             r"thiamine|riboflavine|niacine|acide folique|cyanocobalamine|"
             r"acide ascorbique|cholécalciférol|tocophérol|phytoménadione|"
             r"ferreux|ferrique|fer.*complément|calcium.*complément|zinc.*complément|"
             r"magnésium|multivitamine|prénatale", "Vitamins & Supplements"),
            (r"éthynylestradiol|estradiol|estrogène|lévonorgestrel|norgestrel|"
             r"noréthistérone|désogestrel|drospirénone|étonogestrel|norgestimate|"
             r"progestérone|misoprostol.*obstét|ocytocine|mifépristone|ulipristal|"
             r"clomifène|létrozole.*fertilité|raloxifène|ospémifène", "Women's Health"),
            (r"tamsulosine|alfuzosine|finastéride|dutastéride|sildénafil|tadalafil|"
             r"vardénafil|oxybutynine|toltérodine|solifénacine|mirabégron|"
             r"tamsulosin|alfuzosin|finasteride|dutasteride", "Urology"),
            (r"tamoxifène|anastrozole|létrozole.*cancer|exémestane|fulvestrant|"
             r"imatinib|erlotinib|cyclophosphamide|méthotrexate.*cancer|"
             r"capécitabine|témozolomide|paclitaxel|docétaxel|"
             r"pembrolizumab|nivolumab|bévacizumab", "Oncology"),
            (r"méthotrexate.*rhum|hydroxychloroquine|sulfasalazine.*rhum|"
             r"léflunomide|étanercept|adalimumab|infliximab|"
             r"colchicine|allopurinol|fébuxostat|probénécide|"
             r"cyclobenzaprine|baclofène|tizanidine|"
             r"alendronate|risédronate|acide zolédronique|dénosumab", "Joints & Muscles"),
            (r"trétinoïne|adapalène|benzoyle|isotrétinoïne|clobétasol|"
             r"bétaméthasone.*topique|fluocinonide|tacrolimus.*topique|"
             r"calcipotriol|mupirocine|minoxidil.*topique|"
             r"imiquimod|perméthrine.*topique|acide salicylique.*topique", "Skin & Wounds"),
            (r"latanoprost|bimatoprost|timolol.*ophtalmique|dorzolamide|"
             r"brimonidine|ciprofloxacine.*ophtalmique|tobramycine.*ophtalmique|"
             r"prednisolone.*ophtalmique|dexaméthasone.*ophtalmique|"
             r"olopatadine.*ophtalmique|larmes artificielles|"
             r"néomycine.*otique|ciprofloxacine.*otique", "Eye & Ear"),
            (r"lidocaïne|benzocaïne|bupivacaïne|ropivacaïne|"
             r"chlorhexidine|povidone.*iodée|peroxyde d'hydrogène|"
             r"bacitracine|néomycine.*topique|mupirocine.*plaie", "First Aid"),
        ]

        for pattern, cat in INN_CAT:
            if re.search(pattern, inn_lower, re.I):
                category = cat
                break

        if not category:
            sk_cat += 1; continue

        # Farmaceutische vorm (kol 2)
        form_map = [
            (r"comprimé|cp\b|cpr\b", "Tablet"),
            (r"gélule|capsule", "Capsule"),
            (r"solution buvable|sirop|suspension buvable", "Syrup"),
            (r"collyre|gouttes ophtalmiques", "Eye drops"),
            (r"spray nasal|pulvérisation nasale", "Nasal spray"),
            (r"inhalation|aérosol|poudre.*inhal", "Inhaler"),
            (r"crème\b|cream", "Cream"),
            (r"pommade\b|ointment", "Ointment"),
            (r"gel\b", "Gel"),
            (r"patch|dispositif transdermique", "Patch"),
            (r"injectable|solution injectable|injection", "Injection"),
            (r"suppositoire", "Suppository"),
            (r"poudre\b|powder", "Powder"),
            (r"suspension", "Suspension"),
            (r"solution\b", "Solution"),
            (r"gouttes.*oreille|auriculaire", "Ear drops"),
        ]
        form = "Tablet"
        for pat, f in form_map:
            if re.search(pat, form_raw, re.I):
                form = f; break

        rx = cis in cis_rx

        key = name.lower()
        if key in seen:
            sk_dup += 1; continue
        seen.add(key)

        results.append({
            "Name": name, "INN": inn, "ATC": "",
            "PharmaceuticalForm": form,
            "RxStatus": "Rx" if rx else "OTC",
            "Country": "FR",
        })

    print(f"\n  ✅ {len(results)} medicijnen")
    print(f"     Niet gecommercialiseerd: {sk_status}")
    print(f"     Geen categorie: {sk_cat}")
    print(f"     Blacklist: {sk_bl}")
    print(f"     Duplicaten: {sk_dup}")

    if not results:
        print("❌ Geen resultaten"); sys.exit(1)

    fields = ["Name","INN","ATC","PharmaceuticalForm","RxStatus","Country"]
    with open(OUTPUT_FILE,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(results)
    print(f"\n✅ {len(results)} opgeslagen → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
