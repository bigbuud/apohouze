#!/usr/bin/env python3
"""
apoHouze — Verenigde Staten Medicijnen Fetcher v4
==================================================
Bron: openFDA Human Drug NDC Directory (bulk JSON via S3)
  Manifest: https://api.fda.gov/download.json
  → drug.ndc.partitions[].file  (S3 links, altijd bereikbaar)

STRATEGIE:
  - Manifest ophalen om actuele S3-partitie-URLs te krijgen
  - Elke partitie downloaden en verwerken
  - Per record: ZOWEL brand_name ALS generic_name opslaan als aparte entries
  - Categorisatie via brede generieke naam keyword-matching
    (pharm_class is voor 85% van records leeg, dus niet als primaire bron)
  - Dedup op naam (case-insensitive)

Verwacht resultaat: 15.000-30.000 unieke entries
"""

import sys, os, re, csv, time, subprocess, json, zipfile, io, urllib.request

DEBUG = "--debug" in sys.argv
# Gebruik os.getcwd() want update.js roept dit script aan met cwd=repo_root
# os.path.dirname(__file__) kan afwijken als Python het pad anders resolvet
REPO_ROOT   = os.getcwd()
TMP_DIR     = os.path.join(REPO_ROOT, "data", "_tmp")
OUTPUT_FILE = os.path.join(TMP_DIR, "us_medicines.csv")
os.makedirs(TMP_DIR, exist_ok=True)

FDA_MANIFEST = "https://api.fda.gov/download.json"

# ================================================================
# CATEGORIE-MAPPING op generieke naam (breed, 26 categorieën)
# Matcht op NONPROPRIETARYNAME (alle caps in FDA data)
# Geen \b word boundaries nodig - re.search + re.I is voldoende
# ================================================================
GENERIC_MAP = [
    ("Pain & Fever",
     r"acetaminophen|paracetamol|ibuprofen|naproxen|aspirin|diclofenac|"
     r"celecoxib|meloxicam|ketoprofen|piroxicam|indomethacin|ketorolac|"
     r"tramadol|oxycodone|hydrocodone|codeine|morphine|fentanyl|buprenorphine|"
     r"methadone|hydromorphone|oxymorphone|tapentadol|butorphanol|nalbuphine|"
     r"meperidine|sufentanil|remifentanil|alfentanil|pentazocine|"
     r"diflunisal|etodolac|flurbiprofen|meclofenamate|mefenamic|oxaprozin|"
     r"salsalate|sulindac|tolmetin"),

    ("Antibiotics",
     r"amoxicillin|ampicillin|penicillin|nafcillin|oxacillin|dicloxacillin|"
     r"piperacillin|tazobactam|cephalexin|cefuroxime|ceftriaxone|cefdinir|"
     r"cefadroxil|cefprozil|cefpodoxime|cefaclor|cefazolin|cefixime|"
     r"cefepime|ceftazidime|cefotaxime|ceftaroline|"
     r"azithromycin|clarithromycin|erythromycin|fidaxomicin|"
     r"doxycycline|minocycline|tetracycline|tigecycline|omadacycline|"
     r"ciprofloxacin|levofloxacin|moxifloxacin|ofloxacin|gemifloxacin|delafloxacin|"
     r"trimethoprim|sulfamethoxazole|metronidazole|clindamycin|"
     r"vancomycin|linezolid|tedizolid|daptomycin|oritavancin|dalbavancin|"
     r"nitrofurantoin|fosfomycin|rifampin|rifaximin|isoniazid|ethambutol|"
     r"pyrazinamide|aztreonam|imipenem|meropenem|ertapenem|doripenem|"
     r"gentamicin|tobramycin|amikacin|streptomycin|plazomicin|"
     r"chloramphenicol|colistin|polymyxin.*systemic"),

    ("Antivirals",
     r"acyclovir|valacyclovir|famciclovir|penciclovir|"
     r"oseltamivir|zanamivir|peramivir|baloxavir|"
     r"tenofovir|emtricitabine|efavirenz|lopinavir|ritonavir|atazanavir|"
     r"dolutegravir|bictegravir|raltegravir|elvitegravir|cabotegravir|"
     r"sofosbuvir|ledipasvir|velpatasvir|glecaprevir|pibrentasvir|"
     r"elbasvir|grazoprevir|daclatasvir|simeprevir|"
     r"ribavirin|ganciclovir|valganciclovir|cidofovir|foscarnet|letermovir|"
     r"nirmatrelvir|molnupiravir|remdesivir|"
     r"adefovir|entecavir|lamivudine|abacavir|zidovudine|stavudine|"
     r"didanosine|nevirapine|rilpivirine|etravirine|doravirine|"
     r"darunavir|fosamprenavir|tipranavir|indinavir|saquinavir|nelfinavir|"
     r"maraviroc|enfuvirtide|ibalizumab"),

    ("Antifungals",
     r"fluconazole|itraconazole|voriconazole|posaconazole|ketoconazole|"
     r"isavuconazole|oteseconazole|"
     r"clotrimazole|miconazole|terbinafine|nystatin|amphotericin|"
     r"griseofulvin|econazole|butoconazole|terconazole|efinaconazole|"
     r"ciclopirox|tolnaftate|undecylenic|tavaborole|"
     r"anidulafungin|caspofungin|micafungin|rezafungin"),

    ("Antiparasitics",
     r"ivermectin|mebendazole|albendazole|praziquantel|pyrantel|"
     r"hydroxychloroquine|chloroquine|atovaquone|primaquine|mefloquine|"
     r"permethrin|malathion|spinosad|lindane|pyrethrins|"
     r"tinidazole|nitazoxanide|secnidazole|miltefosine|"
     r"diethylcarbamazine|oxamniquine|triclabendazole"),

    ("Allergy",
     r"loratadine|cetirizine|fexofenadine|levocetirizine|desloratadine|"
     r"diphenhydramine|chlorpheniramine|brompheniramine|clemastine|"
     r"hydroxyzine|promethazine|cyproheptadine|carbinoxamine|"
     r"azelastine|olopatadine|epinastine|alcaftadine|bepotastine|ketotifen|"
     r"triprolidine|dexchlorpheniramine|acrivastine"),

    ("Cough & Cold",
     r"dextromethorphan|guaifenesin|pseudoephedrine|phenylephrine|"
     r"oxymetazoline|xylometazoline|naphazoline|tetrahydrozoline|"
     r"benzonatate|bromhexine|ambroxol|acetylcysteine|carbocisteine|"
     r"ipratropium.*nasal|budesonide.*nasal|fluticasone.*nasal|"
     r"mometasone.*nasal|triamcinolone.*nasal|beclomethasone.*nasal|"
     r"ciclesonide.*nasal|flunisolide.*nasal"),

    ("Lungs & Asthma",
     r"albuterol|salbutamol|levalbuterol|salmeterol|formoterol|indacaterol|"
     r"olodaterol|vilanterol|arformoterol|"
     r"tiotropium|umeclidinium|aclidinium|glycopyrrolate.*pulm|ipratropium.*pulm|"
     r"budesonide.*inhal|fluticasone|beclomethasone.*inhal|"
     r"mometasone.*inhal|ciclesonide.*inhal|flunisolide.*inhal|"
     r"montelukast|zafirlukast|zileuton|"
     r"theophylline|aminophylline|roflumilast|"
     r"omalizumab|mepolizumab|benralizumab|dupilumab|tezepelumab|"
     r"cromolyn.*pulm|nedocromil"),

    ("Stomach & Intestine",
     r"omeprazole|pantoprazole|esomeprazole|lansoprazole|rabeprazole|dexlansoprazole|"
     r"ranitidine|famotidine|cimetidine|nizatidine|"
     r"calcium carbonate|aluminum hydroxide|magnesium hydroxide|sodium bicarbonate.*antacid|"
     r"simethicone|loperamide|bismuth|"
     r"metoclopramide|ondansetron|granisetron|dolasetron|palonosetron|"
     r"prochlorperazine|promethazine.*nausea|aprepitant|fosaprepitant|rolapitant|"
     r"docusate|bisacodyl|senna|lactulose|polyethylene glycol|"
     r"lubiprostone|linaclotide|plecanatide|tegaserod|prucalopride|"
     r"mesalamine|mesalazine|balsalazide|olsalazine|sulfasalazine.*gi|"
     r"budesonide.*gi|infliximab.*gi|vedolizumab|ustekinumab.*gi|"
     r"hyoscyamine|dicyclomine|mebeverine|"
     r"pancrelipase|ursodiol|obeticholic|"
     r"rifaximin|neomycin.*hepatic|lactulose.*hepatic|"
     r"alvimopan|methylnaltrexone|naloxegol|naldemedine"),

    ("Heart & Blood Pressure",
     r"amlodipine|nifedipine|felodipine|nicardipine|isradipine|nisoldipine|"
     r"lisinopril|enalapril|ramipril|captopril|benazepril|fosinopril|"
     r"moexipril|perindopril|quinapril|trandolapril|"
     r"losartan|valsartan|irbesartan|candesartan|olmesartan|telmisartan|"
     r"eprosartan|azilsartan|fimasartan|"
     r"metoprolol|atenolol|bisoprolol|carvedilol|propranolol|labetalol|"
     r"nadolol|acebutolol|betaxolol|pindolol|nebivolol|"
     r"hydrochlorothiazide|chlorthalidone|indapamide|metolazone|"
     r"furosemide|torsemide|bumetanide|ethacrynic|"
     r"spironolactone|eplerenone|triamterene|amiloride|finerenone|"
     r"digoxin|amiodarone|dronedarone|flecainide|propafenone|mexiletine|"
     r"disopyramide|quinidine|procainamide|sotalol|dofetilide|ibutilide|"
     r"diltiazem|verapamil|"
     r"hydralazine|minoxidil.*systemic|isosorbide|nitroglycerin|nitroprusside|"
     r"clonidine|methyldopa|guanfacine.*hypert|moxonidine|"
     r"doxazosin|prazosin|terazosin|"
     r"sacubitril|ivabradine|ranolazine|aliskiren|"
     r"dopamine|dobutamine|milrinone|levosimendan|norepinephrine"),

    ("Cholesterol",
     r"atorvastatin|simvastatin|rosuvastatin|pravastatin|lovastatin|"
     r"fluvastatin|pitavastatin|"
     r"ezetimibe|fenofibrate|gemfibrozil|fenofibric|"
     r"evolocumab|alirocumab|inclisiran|bempedoic|"
     r"colestipol|cholestyramine|colesevelam|"
     r"niacin.*lipid|omega.3|icosapentaenoic|docosahexaenoic|"
     r"lomitapide|mipomersen"),

    ("Anticoagulants",
     r"warfarin|heparin|enoxaparin|dalteparin|fondaparinux|tinzaparin|"
     r"apixaban|rivaroxaban|dabigatran|edoxaban|betrixaban|"
     r"clopidogrel|ticagrelor|prasugrel|ticlopidine|"
     r"dipyridamole|vorapaxar|cilostazol|"
     r"argatroban|bivalirudin|"
     r"alteplase|reteplase|tenecteplase|urokinase|streptokinase|"
     r"pentoxifylline|aspirin.*anticoag"),

    ("Diabetes",
     r"metformin|glipizide|glyburide|glimepiride|glibenclamide|gliquidone|"
     r"pioglitazone|rosiglitazone|"
     r"sitagliptin|saxagliptin|linagliptin|alogliptin|vildagliptin|trelagliptin|"
     r"empagliflozin|canagliflozin|dapagliflozin|ertugliflozin|sotagliflozin|"
     r"liraglutide|semaglutide|exenatide|dulaglutide|albiglutide|lixisenatide|"
     r"tirzepatide|"
     r"insulin|acarbose|miglitol|repaglinide|nateglinide|pramlintide|"
     r"colesevelam.*diabet"),

    ("Thyroid",
     r"levothyroxine|liothyronine|liotrix|thyroid.*dessicated|"
     r"methimazole|propylthiouracil|potassium iodide.*thyroid"),

    ("Corticosteroids",
     r"prednisone|prednisolone|methylprednisolone|dexamethasone|"
     r"hydrocortisone.*systemic|betamethasone.*systemic|triamcinolone.*systemic|"
     r"fludrocortisone|cortisone|deflazacort|budesonide.*systemic"),

    ("Neurology",
     r"levodopa|carbidopa|ropinirole|pramipexole|rasagiline|selegiline.*parkinson|"
     r"entacapone|tolcapone|apomorphine|amantadine|safinamide|"
     r"donepezil|rivastigmine|galantamine|memantine|"
     r"gabapentin|pregabalin|phenytoin|fosphenytoin|valproate|valproic|"
     r"carbamazepine|oxcarbazepine|eslicarbazepine|lamotrigine|topiramate|"
     r"levetiracetam|brivaracetam|zonisamide|lacosamide|perampanel|cenobamate|"
     r"rufinamide|vigabatrin|tiagabine|"
     r"sumatriptan|rizatriptan|zolmitriptan|naratriptan|almotriptan|"
     r"eletriptan|frovatriptan|lasmiditan|ubrogepant|rimegepant|"
     r"ergotamine|dihydroergotamine|"
     r"riluzole|edaravone|nusinersen|risdiplam|onasemnogene|"
     r"baclofen.*neuro|tizanidine|dantrolene.*spasm|"
     r"natalizumab|interferon.*ms|glatiramer|fingolimod|siponimod|"
     r"dimethyl fumarate|ozanimod|ponesimod|ofatumumab.*ms|ocrelizumab"),

    ("Sleep & Sedation",
     r"zolpidem|zaleplon|eszopiclone|"
     r"triazolam|temazepam|flurazepam|quazepam|estazolam|"
     r"diazepam|lorazepam|alprazolam|clonazepam|midazolam|chlordiazepoxide|"
     r"oxazepam|clorazepate|"
     r"buspirone|melatonin.*sleep|ramelteon|suvorexant|lemborexant|"
     r"doxepin.*sleep|diphenhydramine.*sleep|hydroxyzine.*sleep|"
     r"phenobarbital|chloral hydrate"),

    ("Antidepressants",
     r"sertraline|fluoxetine|paroxetine|escitalopram|citalopram|fluvoxamine|"
     r"venlafaxine|duloxetine|desvenlafaxine|levomilnacipran|milnacipran|"
     r"bupropion|mirtazapine|trazodone|nefazodone|vilazodone|vortioxetine|"
     r"amitriptyline|nortriptyline|imipramine|desipramine|clomipramine|"
     r"doxepin.*antidepr|trimipramine|protriptyline|"
     r"phenelzine|tranylcypromine|isocarboxazid|selegiline.*antidepr|"
     r"lithium|"
     r"quetiapine|aripiprazole|olanzapine|risperidone|paliperidone|"
     r"ziprasidone|lurasidone|asenapine|iloperidone|brexpiprazole|"
     r"cariprazine|lumateperone|pimavanserin|"
     r"haloperidol|chlorpromazine|thioridazine|fluphenazine|perphenazine|"
     r"thiothixene|loxapine|molindone|clozapine|"
     r"esketamine|ketamine.*depress|gepirone|"
     r"valbenazine|deutetrabenazine"),

    ("Vitamins & Supplements",
     r"vitamin a |vitamin b|vitamin c |vitamin d|vitamin e |vitamin k|"
     r"thiamine|riboflavin|niacin.*vitamin|pyridoxine|biotin|pantothenic|"
     r"folic acid|cyanocobalamin|hydroxocobalamin|methylcobalamin|"
     r"ascorbic acid|cholecalciferol|ergocalciferol|tocopherol|phytonadione|"
     r"ferrous|ferric|iron.*supplement|polysaccharide.*iron|"
     r"calcium carbonate.*supplement|calcium citrate|calcium gluconate|"
     r"calcium acetate.*supplement|"
     r"zinc.*supplement|magnesium.*supplement|potassium chloride.*supplement|"
     r"selenium.*supplement|chromium.*supplement|"
     r"multivitamin|prenatal.*vitamin|"
     r"sodium fluoride.*supplement|fluoride.*supplement"),

    ("Women's Health",
     r"ethinyl estradiol|estradiol|conjugated estrogen|esterified estrogen|"
     r"estrone|estriol|"
     r"medroxyprogesterone|levonorgestrel|norethindrone|desogestrel|"
     r"drospirenone|etonogestrel|norgestimate|norgestrel|dienogest|"
     r"progesterone|hydroxyprogesterone|"
     r"clomiphene|letrozole.*fertility|gonadotropin|follitropin|choriogonadotropin|"
     r"misoprostol.*obstet|dinoprostone|oxytocin|carboprost|methylergonovine|"
     r"mifepristone|ulipristal|"
     r"raloxifene|ospemifene|bazedoxifene|"
     r"danazol|leuprolide|nafarelin|ganirelix|cetrorelix"),

    ("Urology",
     r"tamsulosin|alfuzosin|silodosin|doxazosin.*bph|terazosin.*bph|"
     r"finasteride|dutasteride|"
     r"sildenafil|tadalafil|vardenafil|avanafil|"
     r"oxybutynin|tolterodine|solifenacin|darifenacin|fesoterodine|"
     r"trospium|mirabegron|vibegron|"
     r"bethanechol|phenazopyridine|flavoxate|"
     r"desmopressin.*uro"),

    ("Oncology",
     r"tamoxifen|anastrozole|letrozole.*cancer|exemestane|fulvestrant|toremifene|"
     r"imatinib|erlotinib|gefitinib|osimertinib|afatinib|dacomitinib|"
     r"dasatinib|nilotinib|ponatinib|bosutinib|asciminib|"
     r"ibrutinib|acalabrutinib|zanubrutinib|"
     r"venetoclax|imatinib|"
     r"bortezomib|carfilzomib|ixazomib|"
     r"lenalidomide|thalidomide|pomalidomide|"
     r"cyclophosphamide|ifosfamide|melphalan|busulfan|"
     r"methotrexate.*cancer|fluorouracil|capecitabine|gemcitabine|"
     r"temozolomide|carmustine|lomustine|"
     r"paclitaxel|docetaxel|cabazitaxel|nab.paclitaxel|"
     r"irinotecan|topotecan|etoposide|"
     r"pembrolizumab|nivolumab|atezolizumab|durvalumab|avelumab|"
     r"ipilimumab|cemiplimab|tremelimumab|"
     r"bevacizumab|ramucirumab|sunitinib|sorafenib|regorafenib|cabozantinib|"
     r"palbociclib|ribociclib|abemaciclib|"
     r"olaparib|niraparib|rucaparib|talazoparib|"
     r"abiraterone|enzalutamide|darolutamide|apalutamide|"
     r"trastuzumab|pertuzumab|ado.trastuzumab|"
     r"rituximab.*cancer|obinutuzumab|ofatumumab.*cancer|"
     r"blinatumomab|inotuzumab|gemtuzumab"),

    ("Joints & Muscles",
     r"methotrexate.*rheuma|hydroxychloroquine|sulfasalazine.*rheuma|"
     r"leflunomide|etanercept|adalimumab|infliximab|golimumab|certolizumab|"
     r"tocilizumab|sarilumab|abatacept|rituximab.*rheuma|"
     r"baricitinib|tofacitinib|upadacitinib|filgotinib|"
     r"colchicine|allopurinol|febuxostat|probenecid|rasburicase|pegloticase|"
     r"cyclobenzaprine|methocarbamol|carisoprodol|orphenadrine|chlorzoxazone|"
     r"baclofen.*muscle|tizanidine|dantrolene.*muscle|"
     r"alendronate|risedronate|ibandronate|zoledronic|pamidronate|"
     r"denosumab|teriparatide|abaloparatide|romosozumab|"
     r"indomethacin.*gout|sulindac|nabumetone"),

    ("Skin & Wounds",
     r"tretinoin|adapalene|tazarotene|trifarotene|"
     r"benzoyl peroxide|salicylic acid.*topical|azelaic acid|"
     r"clindamycin.*topical|erythromycin.*topical|dapsone.*topical|"
     r"isotretinoin|acitretin|alitretinoin|"
     r"clobetasol|halobetasol|betamethasone.*topical|mometasone.*topical|"
     r"fluocinonide|triamcinolone.*topical|hydrocortisone.*topical|"
     r"desonide|fluocinolone.*topical|alclometasone|diflorasone|"
     r"tacrolimus.*topical|pimecrolimus|"
     r"calcipotriene|calcitriol.*topical|"
     r"mupirocin|fusidic acid|retapamulin|"
     r"minoxidil.*topical|"
     r"imiquimod|podofilox|sinecatechins|"
     r"ivermectin.*topical|permethrin.*topical|malathion.*topical|"
     r"coal tar|anthralin|urea.*topical|lactic acid.*topical|"
     r"silver sulfadiazine|mafenide"),

    ("Eye & Ear",
     r"latanoprost|bimatoprost|travoprost|tafluprost|unoprostone|"
     r"timolol.*ophthal|betaxolol.*ophthal|carteolol.*ophthal|"
     r"dorzolamide|brinzolamide|acetazolamide.*eye|methazolamide|"
     r"brimonidine|apraclonidine|"
     r"pilocarpine.*eye|echothiophate|"
     r"ciprofloxacin.*ophthal|ofloxacin.*ophthal|moxifloxacin.*ophthal|"
     r"levofloxacin.*ophthal|tobramycin.*ophthal|gentamicin.*ophthal|"
     r"azithromycin.*ophthal|erythromycin.*ophthal|bacitracin.*ophthal|"
     r"prednisolone.*ophthal|dexamethasone.*ophthal|fluorometholone|"
     r"loteprednol|difluprednate|rimexolone|"
     r"ketorolac.*ophthal|diclofenac.*ophthal|bromfenac|nepafenac|"
     r"olopatadine.*ophthal|ketotifen.*ophthal|azelastine.*ophthal|"
     r"cyclopentolate|tropicamide|atropine.*ophthal|homatropine|"
     r"artificial tears|carboxymethylcellulose.*eye|"
     r"hyaluronate.*eye|polyvinyl alcohol.*eye|hydroxypropyl.*eye|"
     r"neomycin.*otic|ciprofloxacin.*otic|ofloxacin.*otic|"
     r"acetic acid.*otic|antipyrine.*otic|benzocaine.*otic|"
     r"hydrocortisone.*otic|dexamethasone.*otic"),

    ("First Aid",
     r"lidocaine|benzocaine|prilocaine|tetracaine|bupivacaine|ropivacaine|"
     r"procaine|mepivacaine|articaine|"
     r"chlorhexidine|povidone.iodine|hydrogen peroxide.*topical|"
     r"isopropyl alcohol.*topical|ethanol.*topical|"
     r"bacitracin.*topical|polymyxin.*topical|neomycin.*topical|"
     r"mupirocin.*wound|retapamulin.*topical|"
     r"collagenase.*wound|becaplermin|"
     r"benzalkonium|cetylpyridinium|"
     r"epinephrine.*topical|thrombin.*topical"),
]

BLACKLIST = re.compile(
    r"\b(vaccine|vaccin|immunoglobulin|antitoxin|antivenom|"
    r"whole blood|packed red|platelet concentrate|plasma.*transfus|albumin.*transfus|"
    r"diagnostic.*kit|in vitro|reagent kit|contrast.*media|radiolabeled|radioactive|"
    r"dialysis.*solution|peritoneal.*dialysis)\b", re.I
)


def to_category(name):
    """Generieke naam → categorie via keyword matching."""
    if not name:
        return None
    t = name.lower()
    for cat, pat in GENERIC_MAP:
        if re.search(pat, t, re.I):
            return cat
    return None


def curl_download(url, dest, max_time=300):
    cmd = ["curl", "-L", "--max-time", str(max_time), "--connect-timeout", "20",
           "--silent", "--fail", "--user-agent", "Mozilla/5.0 apoHouze-updater/5.0",
           "-o", dest, url]
    for attempt in range(3):
        try:
            subprocess.run(cmd, timeout=max_time + 15, check=True)
            size = os.path.getsize(dest)
            print(f"    ✅ {size // 1024} KB")
            return size
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"    ⚠️  Poging {attempt+1}/3: {e}")
            if attempt < 2:
                time.sleep(5)
    return 0


def get_manifest():
    """Haal openFDA download manifest op → lijst van partitie-URLs."""
    print(f"  🌐 Manifest: {FDA_MANIFEST}")
    req = urllib.request.Request(
        FDA_MANIFEST,
        headers={"User-Agent": "Mozilla/5.0 apoHouze-updater/5.0"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    ndc   = data["results"]["drug"]["ndc"]
    parts = ndc["partitions"]
    total = ndc.get("total_records", "?")
    print(f"  📊 {len(parts)} partities, ~{total} records")
    return parts


def process_partition(path, seen):
    """Verwerk één openFDA JSON-partitie, retourneer nieuwe entries."""
    if path.endswith(".zip"):
        with zipfile.ZipFile(path) as z:
            jname = next(n for n in z.namelist() if n.endswith(".json"))
            with z.open(jname) as f:
                items = json.loads(f.read())["results"]
    else:
        with open(path, encoding="utf-8") as f:
            items = json.load(f)["results"]

    entries  = []
    sk_bl    = 0
    sk_cat   = 0
    sk_dup   = 0

    for item in items:
        brand   = (item.get("brand_name") or "").strip()
        generic = (item.get("generic_name") or "").strip()
        form    = (item.get("dosage_form") or "").strip()
        dea     = (item.get("dea_schedule") or "").strip()
        mkt     = (item.get("marketing_category") or "").upper()
        rx      = bool(dea) or ("OTC" not in mkt and "MONOGRAPH" not in mkt)

        # Blacklist check
        if BLACKLIST.search(brand) or BLACKLIST.search(generic):
            sk_bl += 1
            continue

        # Categorie: probeer op generic naam, dan op brand naam
        category = to_category(generic) or to_category(brand)
        if not category:
            sk_cat += 1
            continue

        # Sla BRAND op (indien aanwezig en uniek)
        if brand:
            key = brand.lower()
            if key not in seen:
                seen.add(key)
                entries.append({"Name": brand, "INN": generic, "ATC": "",
                                 "PharmaceuticalForm": form,
                                 "RxStatus": "Rx" if rx else "OTC",
                                 "Country": "US", "Category": category})
            else:
                sk_dup += 1

        # Sla GENERIC apart op (indien verschilt van brand)
        if generic and generic.lower() != brand.lower():
            key = generic.lower()
            if key not in seen:
                seen.add(key)
                entries.append({"Name": generic, "INN": generic, "ATC": "",
                                 "PharmaceuticalForm": form,
                                 "RxStatus": "Rx" if rx else "OTC",
                                 "Country": "US", "Category": category})
            else:
                sk_dup += 1

    if DEBUG:
        print(f"    → {len(entries)} nieuw | {sk_cat} geen cat | {sk_bl} blacklist | {sk_dup} dup")
    return entries


def save_csv(rows):
    fields = ["Name", "INN", "ATC", "PharmaceuticalForm", "RxStatus", "Country", "Category"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"\n✅ {len(rows)} medicijnen opgeslagen → {OUTPUT_FILE}")


def main():
    print("🇺🇸 apoHouze — Verenigde Staten Medicijnen Fetcher v4")
    print("=" * 54)
    print("📌 Bron: openFDA NDC JSON bulk (api.fda.gov → S3)\n")

    print("[1/3] Manifest ophalen...")
    try:
        partitions = get_manifest()
    except Exception as e:
        print(f"❌ Manifest mislukt: {e}")
        sys.exit(1)

    seen    = set()
    all_rows = []

    print(f"\n[2/3] {len(partitions)} partities downloaden en verwerken...")
    for i, part in enumerate(partitions):
        url     = part["file"]
        records = part.get("records", "?")
        print(f"\n  📦 Partitie {i+1}/{len(partitions)} (~{records} records)")
        print(f"    📥 {url}")

        dest = os.path.join(TMP_DIR, f"us_ndc_p{i+1}.zip")
        size = curl_download(url, dest)
        if size < 1000:
            print(f"    ⚠️  Download mislukt, overgeslagen")
            continue

        try:
            new = process_partition(dest, seen)
            all_rows.extend(new)
            print(f"    ✅ +{len(new)} entries (totaal: {len(all_rows)})")
        except Exception as e:
            print(f"    ⚠️  Verwerking mislukt: {e}")
        finally:
            try:
                os.remove(dest)
            except Exception:
                pass

    print(f"\n  🎯 Totaal uniek: {len(all_rows)}")

    if not all_rows:
        print("❌ Geen resultaten")
        sys.exit(1)

    print(f"\n[3/3] Opslaan...")
    save_csv(all_rows)


if __name__ == "__main__":
    main()
