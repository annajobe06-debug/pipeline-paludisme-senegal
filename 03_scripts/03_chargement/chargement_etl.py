# =============================================================
# PROJET   : Pipeline Paludisme Sénégal
# ÉTAPE 4  : Chargement ETL Python → SQL Server
# VERSION  : 2.1 
# Auteur   : Anna JOBE
# Date     : 2026
# =============================================================

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib
import os

STAGING_PATH = r"C:\Users\jobea\OneDrive\Documents\Pipeline_paludisme_senegal\02_staging"

# ── Connexion SQL Server ──────────────────────────────────────
print("\n" + "="*60)
print("  CONNEXION A SQL SERVER")
print("="*60)

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=ANNA\\SQLEXPRESS;"
    "DATABASE=paludisme_senegal;"
    "UID=pipeline_user;"
    "PWD=paludisme;"
    "TrustServerCertificate=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

try:
    with engine.connect() as conn:
        print("   Connexion reussie a paludisme_senegal !")
except Exception as e:
    print(f"   Erreur : {e}")
    exit()

# =============================================================
# CHARGEMENT 1 — dim_date
# =============================================================
print("\n" + "="*60)
print("  CHARGEMENT 1 : dim_date")
print("="*60)

dim_date_rows = []
for annee in range(2000, 2025):
    for trimestre in range(1, 5):
        saison  = "Saison des pluies" if trimestre in [2, 3] else "Saison seche"
        id_date = int(f"{annee}{trimestre}")
        dim_date_rows.append({
            "id_date"   : id_date,
            "annee"     : annee,
            "trimestre" : trimestre,
            "saison"    : saison
        })

df_date = pd.DataFrame(dim_date_rows)
df_date.to_sql(
    name="dim_date", con=engine,
    if_exists="append", index=False
)
print(f"   {len(df_date)} lignes → dim_date")

# =============================================================
# CHARGEMENT 2 — dim_region
# =============================================================
print("\n" + "="*60)
print("  CHARGEMENT 2 : dim_region")
print("="*60)

regions_senegal = [
    {"nom_region": "Dakar",       "zone_geo": "Ouest"},
    {"nom_region": "Thies",       "zone_geo": "Ouest"},
    {"nom_region": "Diourbel",    "zone_geo": "Centre"},
    {"nom_region": "Fatick",      "zone_geo": "Centre"},
    {"nom_region": "Kaolack",     "zone_geo": "Centre"},
    {"nom_region": "Kaffrine",    "zone_geo": "Centre"},
    {"nom_region": "Saint-Louis", "zone_geo": "Nord"},
    {"nom_region": "Louga",       "zone_geo": "Nord"},
    {"nom_region": "Matam",       "zone_geo": "Nord"},
    {"nom_region": "Tambacounda", "zone_geo": "Est"},
    {"nom_region": "Kedougou",    "zone_geo": "Est"},
    {"nom_region": "Kolda",       "zone_geo": "Sud"},
    {"nom_region": "Sedhiou",     "zone_geo": "Sud"},
    {"nom_region": "Ziguinchor",  "zone_geo": "Sud"},
]

df_region         = pd.DataFrame(regions_senegal)
df_region["pays"] = "Senegal"
df_region.to_sql(
    name="dim_region", con=engine,
    if_exists="append", index=False
)
print(f"   {len(df_region)} regions → dim_region")

# =============================================================
# CHARGEMENT 3 — dim_indicateur
# =============================================================
print("\n" + "="*60)
print("  CHARGEMENT 3 : dim_indicateur")
print("="*60)

indicateurs = [
    {"code_indicateur": "NB_CAS_CONFIRMES",   "nom_indicateur": "Number of confirmed malaria cases",                         "categorie": "Epidemiologie", "unite": "Nombre absolu"},
    {"code_indicateur": "NB_CAS_PRESUMES",    "nom_indicateur": "Number of presumed malaria cases",                          "categorie": "Epidemiologie", "unite": "Nombre absolu"},
    {"code_indicateur": "TAUX_INCIDENCE",     "nom_indicateur": "Estimated malaria incidence (per 1000 population at risk)", "categorie": "Epidemiologie", "unite": "Pour 1 000 habitants"},
    {"code_indicateur": "PREVALENCE_RDT",     "nom_indicateur": "Malaria prevalence according to RDT",                      "categorie": "Epidemiologie", "unite": "Pourcentage (%)"},
    {"code_indicateur": "TAUX_MORTALITE",     "nom_indicateur": "Estimated malaria mortality rate (per 100 000 population)", "categorie": "Mortalite",     "unite": "Pour 100 000 habitants"},
    {"code_indicateur": "TAUX_TRAIT_ENFANTS", "nom_indicateur": "Children with fever receiving antimalarial drugs",          "categorie": "Prevention",    "unite": "Pourcentage (%)"},
    {"code_indicateur": "TAUX_TPI_GROSSESSE", "nom_indicateur": "Intermittent preventive treatment (IPT) of malaria",       "categorie": "Prevention",    "unite": "Pourcentage (%)"},
]

df_indicateur = pd.DataFrame(indicateurs)
df_indicateur.to_sql(
    name="dim_indicateur", con=engine,
    if_exists="append", index=False
)
print(f"   {len(df_indicateur)} indicateurs → dim_indicateur")

# =============================================================
# CHARGEMENT 4 — dim_source
# =============================================================
print("\n" + "="*60)
print("  CHARGEMENT 4 : dim_source")
print("="*60)

sources = [
    {"nom_source": "OMS / HDX",      "organisme": "Organisation Mondiale de la Sante",          "url": "https://data.humdata.org/dataset/who-data-for-senegal"},
    {"nom_source": "DHS / HDX",      "organisme": "DHS Program - Demographic and Health Surveys","url": "https://data.humdata.org/dataset/dhs-subnational-data-for-senegal"},
    {"nom_source": "World Bank / HDX","organisme": "Banque Mondiale",                             "url": "https://data.worldbank.org/country/senegal"},
]

df_source = pd.DataFrame(sources)
df_source.to_sql(
    name="dim_source", con=engine,
    if_exists="append", index=False
)
print(f"   {len(df_source)} sources → dim_source")

# =============================================================
# CHARGEMENT 5 — dim_structure_sanitaire
# =============================================================
print("\n" + "="*60)
print("  CHARGEMENT 5 : dim_structure_sanitaire")
print("="*60)

df_facilities = pd.read_csv(
    os.path.join(STAGING_PATH, "clean_health_facilities_senegal.csv")
)

with engine.connect() as conn:
    df_regions_sql = pd.read_sql(
        text("SELECT id_region, nom_region FROM dim_region"), conn
    )

df_facilities = df_facilities.merge(
    df_regions_sql,
    left_on="region", right_on="nom_region", how="left"
)

non_matches = df_facilities[df_facilities["id_region"].isna()]["region"].unique()
if len(non_matches) > 0:
    print(f"   Regions non matchees : {non_matches}")

df_facilities_final = df_facilities[[
    "nom_structure", "type_structure", "gestion",
    "latitude", "longitude", "id_region"
]].dropna(subset=["id_region"])

df_facilities_final["id_region"] = df_facilities_final["id_region"].astype(int)
df_facilities_final.to_sql(
    name="dim_structure_sanitaire", con=engine,
    if_exists="append", index=False
)
print(f"   {len(df_facilities_final)} structures → dim_structure_sanitaire")

# =============================================================
# CHARGEMENT 6 — fact_paludisme (modèle long)
# Grain : 1 ligne = 1 indicateur + 1 région + 1 date + 1 source
# =============================================================
print("\n" + "="*60)
print("  CHARGEMENT 6 : fact_paludisme (modele long)")
print("="*60)

with engine.connect() as conn:
    df_regions_sql     = pd.read_sql(text("SELECT id_region, nom_region FROM dim_region"), conn)
    df_indicateurs_sql = pd.read_sql(text("SELECT id_indicateur, code_indicateur FROM dim_indicateur"), conn)
    df_sources_sql     = pd.read_sql(text("SELECT id_source, nom_source FROM dim_source"), conn)

def get_id_indicateur(code):
    result = df_indicateurs_sql[df_indicateurs_sql["code_indicateur"] == code]["id_indicateur"].values
    return int(result[0]) if len(result) > 0 else None

def get_id_source(nom):
    result = df_sources_sql[df_sources_sql["nom_source"] == nom]["id_source"].values
    return int(result[0]) if len(result) > 0 else None

def get_id_region(nom):
    result = df_regions_sql[df_regions_sql["nom_region"] == nom]["id_region"].values
    return int(result[0]) if len(result) > 0 else None

def get_id_date(annee):
    return int(f"{annee}1")

fact_rows = []

# ── SOURCE 1 : malaria_indicators — données nationales ────────
print("  → Chargement malaria_indicators (national)...")

df_national       = pd.read_csv(os.path.join(STAGING_PATH, "transformed_malaria_national.csv"))
id_source_oms     = get_id_source("OMS / HDX")
id_region_national = get_id_region("Dakar")

mapping_national = {
    "nb_cas_confirmes" : "NB_CAS_CONFIRMES",
    "nb_cas_presumes"  : "NB_CAS_PRESUMES",
    "taux_incidence"   : "TAUX_INCIDENCE",
    "taux_mortalite"   : "TAUX_MORTALITE",
}

for _, row in df_national.iterrows():
    annee   = int(row["annee"])
    id_date = get_id_date(annee)

    for col, code in mapping_national.items():
        valeur = row.get(col)
        if pd.isna(valeur):
            continue

        id_indicateur = get_id_indicateur(code)
        if id_indicateur is None:
            continue

        valeur_basse = row.get("valeur_basse") if code == "TAUX_INCIDENCE" else None
        valeur_haute = row.get("valeur_haute") if code == "TAUX_INCIDENCE" else None

        fact_rows.append({
            "id_date"            : id_date,
            "id_indicateur"      : id_indicateur,
            "id_source"          : id_source_oms,
            "id_region"          : id_region_national,
            "valeur"             : float(valeur),
            "valeur_basse"       : float(valeur_basse) if valeur_basse and not pd.isna(valeur_basse) else None,
            "valeur_haute"       : float(valeur_haute) if valeur_haute and not pd.isna(valeur_haute) else None,
            "population_enquete" : None,
        })

print(f"     {len(fact_rows)} lignes depuis malaria_national")

# ── SOURCE 2 : malaria_subnational — données régionales ───────
print("  → Chargement malaria_subnational (regional)...")

df_regional   = pd.read_csv(os.path.join(STAGING_PATH, "transformed_malaria_regional.csv"))
id_source_dhs = get_id_source("DHS / HDX")
id_ind_rdt    = get_id_indicateur("PREVALENCE_RDT")
count_before  = len(fact_rows)

for _, row in df_regional.iterrows():
    id_region = get_id_region(row["region"])
    if id_region is None:
        continue

    valeur = row.get("prevalence_rdt")
    if pd.isna(valeur):
        continue

    valeur_basse = row.get("valeur_basse")
    valeur_haute = row.get("valeur_haute")
    pop          = row.get("population_enquetee")

    fact_rows.append({
        "id_date"            : get_id_date(int(row["annee"])),
        "id_indicateur"      : id_ind_rdt,
        "id_source"          : id_source_dhs,
        "id_region"          : id_region,
        "valeur"             : float(valeur),
        "valeur_basse"       : float(valeur_basse) if valeur_basse and not pd.isna(valeur_basse) else None,
        "valeur_haute"       : float(valeur_haute) if valeur_haute and not pd.isna(valeur_haute) else None,
        "population_enquete" : float(pop) if pop and not pd.isna(pop) else None,
    })

print(f"     {len(fact_rows) - count_before} lignes depuis malaria_regional")

# ── SOURCE 3 : health_malaria — prévention ────────────────────
print("  → Chargement health_malaria (prevention)...")

df_prevention  = pd.read_csv(os.path.join(STAGING_PATH, "transformed_prevention.csv"))
id_source_wb   = get_id_source("World Bank / HDX")
count_before   = len(fact_rows)

mapping_prevention = {
    "taux_traitement_enfants" : "TAUX_TRAIT_ENFANTS",
    "taux_tpi_grossesse"      : "TAUX_TPI_GROSSESSE",
}

for _, row in df_prevention.iterrows():
    annee   = int(row["annee"])
    id_date = get_id_date(annee)

    for col, code in mapping_prevention.items():
        valeur = row.get(col)
        if pd.isna(valeur):
            continue

        id_indicateur = get_id_indicateur(code)
        if id_indicateur is None:
            continue

        fact_rows.append({
            "id_date"            : id_date,
            "id_indicateur"      : id_indicateur,
            "id_source"          : id_source_wb,
            "id_region"          : id_region_national,
            "valeur"             : float(valeur),
            "valeur_basse"       : None,
            "valeur_haute"       : None,
            "population_enquete" : None,
        })

print(f"     {len(fact_rows) - count_before} lignes depuis prevention")

# ── Chargement final ──────────────────────────────────────────
df_fact = pd.DataFrame(fact_rows)
df_fact.to_sql(
    name="fact_paludisme", con=engine,
    if_exists="append", index=False
)
print(f"   {len(df_fact)} lignes totales → fact_paludisme")

# =============================================================
# BILAN FINAL
# =============================================================
print("\n" + "="*60)
print("  BILAN FINAL DU CHARGEMENT ETL")
print("="*60)

with engine.connect() as conn:
    tables = [
        "dim_date", "dim_region", "dim_indicateur",
        "dim_source", "dim_structure_sanitaire", "fact_paludisme"
    ]
    for table in tables:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"  {table:<35} → {count} lignes")

print("\n ETL termine. Base prete pour Power BI !")
