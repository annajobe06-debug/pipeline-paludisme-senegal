# =============================================================
# PROJET  : Pipeline Paludisme Sénégal
# ÉTAPE 1 : Nettoyage des données
# VERSION : 3.0 — Correction encodage et accents
# Auteur  : Anna JOBE
# Date    : 2026
# =============================================================

import pandas as pd
import numpy as np
import unicodedata
import os

RAW_DATA_PATH     = r"C:\Users\jobea\OneDrive\Documents\Pipeline_paludisme_senegal\01_raw_data"
STAGING_DATA_PATH = r"C:\Users\jobea\OneDrive\Documents\Pipeline_paludisme_senegal\02_staging"

os.makedirs(STAGING_DATA_PATH, exist_ok=True)

def supprimer_accents(texte):
    if pd.isna(texte):
        return texte
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(texte))
        if unicodedata.category(c) != 'Mn'
    )

# =============================================================
# DATASET 1 — malaria_indicators_sen.csv
# =============================================================
print("\n" + "="*60)
print("  NETTOYAGE 1 : malaria_indicators_sen.csv")
print("="*60)

df_malaria = pd.read_csv(
    os.path.join(RAW_DATA_PATH, "malaria_indicators_sen.csv"),
    encoding='utf-8'
)

cols_utiles = ['GHO (CODE)', 'GHO (DISPLAY)', 'YEAR (DISPLAY)', 'Numeric', 'Value', 'Low', 'High']
df_malaria  = df_malaria[cols_utiles]

df_malaria.rename(columns={
    'GHO (CODE)'    : 'indicateur_code',
    'GHO (DISPLAY)' : 'indicateur_nom',
    'YEAR (DISPLAY)': 'annee',
    'Numeric'       : 'valeur_numerique',
    'Value'         : 'valeur_texte',
    'Low'           : 'valeur_basse',
    'High'          : 'valeur_haute'
}, inplace=True)

df_malaria['annee']          = df_malaria['annee'].astype(int)
df_malaria['pays']           = 'Senegal'
df_malaria['indicateur_nom'] = df_malaria['indicateur_nom'].str.strip()
df_malaria['indicateur_nom'] = df_malaria['indicateur_nom'].str.replace(r'\s+', ' ', regex=True)

df_malaria.to_csv(
    os.path.join(STAGING_DATA_PATH, "clean_malaria_indicators.csv"),
    index=False, encoding='utf-8'
)
print(f"   {len(df_malaria)} lignes → clean_malaria_indicators.csv")

# =============================================================
# DATASET 2 — malaria_subnational_sen.csv
# =============================================================
print("\n" + "="*60)
print("  NETTOYAGE 2 : malaria_subnational_sen.csv")
print("="*60)

df_sub = pd.read_csv(
    os.path.join(RAW_DATA_PATH, "malaria-parasitemia_subnational_sen.csv"),
    encoding='utf-8'
)

cols_utiles = ['Location', 'Indicator', 'Value', 'SurveyYear',
               'CharacteristicLabel', 'DenominatorWeighted', 'CILow', 'CIHigh']
df_sub = df_sub[cols_utiles]

df_sub.rename(columns={
    'Location'           : 'region',
    'Indicator'          : 'indicateur',
    'Value'              : 'prevalence_pct',
    'SurveyYear'         : 'annee',
    'CharacteristicLabel': 'label_region',
    'DenominatorWeighted': 'population_enquetee',
    'CILow'              : 'intervalle_bas',
    'CIHigh'             : 'intervalle_haut'
}, inplace=True)

df_sub['region']       = df_sub['region'].str.replace(r'^\.\.*', '', regex=True).str.strip()
df_sub['label_region'] = df_sub['label_region'].str.replace(r'^\.\.*', '', regex=True).str.strip()
df_sub['region']       = df_sub['region'].apply(supprimer_accents)
df_sub['label_region'] = df_sub['label_region'].apply(supprimer_accents)

mapping_regions = {
    'Nord et Est'       : 'Nord et Est',
    'Saint Louis'       : 'Saint-Louis',
    'Louga'             : 'Louga',
    'Matam'             : 'Matam',
    'Tambacounda (2010)': 'Tambacounda',
    'Tambacounda (2005)': 'Tambacounda',
    'Kaffrine'          : 'Kaffrine',
    'Kaolack (2010)'    : 'Kaolack',
    'Kaolack (2005)'    : 'Kaolack',
    'Fatick'            : 'Fatick',
    'Thies'             : 'Thies',
    'Dakar'             : 'Dakar',
    'Diourbel'          : 'Diourbel',
    'Kolda (2010)'      : 'Kolda',
    'Kolda (2005)'      : 'Kolda',
    'Ziguinchor'        : 'Ziguinchor',
    'Sedhiou'           : 'Sedhiou',
    'Kedougou'          : 'Kedougou',
}
df_sub['region'] = df_sub['region'].replace(mapping_regions)
df_sub['pays']   = 'Senegal'

print(f"   {len(df_sub)} lignes → clean_malaria_subnational.csv")
print(f"   Regions : {sorted(df_sub['region'].unique())}")

df_sub.to_csv(
    os.path.join(STAGING_DATA_PATH, "clean_malaria_subnational.csv"),
    index=False, encoding='utf-8'
)

# =============================================================
# DATASET 3 — health_sen.csv
# =============================================================
print("\n" + "="*60)
print("  NETTOYAGE 3 : health_sen.csv → filtre paludisme")
print("="*60)

df_health = pd.read_csv(
    os.path.join(RAW_DATA_PATH, "health_sen.csv"),
    encoding='utf-8'
)

df_health_malaria = df_health[
    df_health['Indicator Name'].str.contains('malaria|Malaria', na=False)
].copy()

df_health_malaria.rename(columns={
    'Country Name'   : 'pays',
    'Country ISO3'   : 'code_pays',
    'Year'           : 'annee',
    'Indicator Name' : 'indicateur_nom',
    'Indicator Code' : 'indicateur_code',
    'Value'          : 'valeur'
}, inplace=True)

df_health_malaria = df_health_malaria.dropna(subset=['valeur'])
df_health_malaria['indicateur_nom'] = df_health_malaria['indicateur_nom'].str.strip()
df_health_malaria['indicateur_nom'] = df_health_malaria['indicateur_nom'].str.replace(r'\s+', ' ', regex=True)
df_health_malaria['pays'] = 'Senegal'

print(f"   {len(df_health_malaria)} lignes → clean_health_malaria.csv")

df_health_malaria.to_csv(
    os.path.join(STAGING_DATA_PATH, "clean_health_malaria.csv"),
    index=False, encoding='utf-8'
)

# =============================================================
# DATASET 4 — health_facilities.xlsx
# =============================================================
print("\n" + "="*60)
print("  NETTOYAGE 4 : health_facilities.xlsx → filtre Senegal")
print("="*60)

df_fac     = pd.read_excel(os.path.join(RAW_DATA_PATH, "sub-saharan_health_facilities.xlsx"))
df_fac_sen = df_fac[df_fac['Country'] == 'Senegal'].copy()

avant = len(df_fac_sen)
df_fac_sen = df_fac_sen.drop_duplicates()
print(f"  Doublons supprimes : {avant - len(df_fac_sen)}")

df_fac_sen.rename(columns={
    'Country'    : 'pays',
    'Admin1'     : 'region',
    'Facility_n' : 'nom_structure',
    'Facility_t' : 'type_structure',
    'Ownership'  : 'gestion',
    'Lat'        : 'latitude',
    'Long'       : 'longitude',
    'LL_source'  : 'source_coordonnees'
}, inplace=True)

# Correction encodage via mapping direct
mapping_types = {
    'H+|pital'           : 'Hopital',
    'H+|pital G+n+ral'   : 'Hopital General',
    'H+|pital R+gional'  : 'Hopital Regional',
    'Poste de Sant+¬'    : 'Poste de Sante',
    'Centre de Sant+¬'   : 'Centre de Sante',
    'H+¦pital'           : 'Hopital',
    'H+¦pital R+¬gional' : 'Hopital Regional',
    'H+¦pital G+¬n+¬ral' : 'Hopital General',
}

#  Suppression accents sur toutes les colonnes texte
for col in ['nom_structure', 'type_structure', 'gestion']:
    df_fac_sen[col] = df_fac_sen[col].apply(supprimer_accents)

#  Remplacement des types restants non corrigés
df_fac_sen['type_structure'] = df_fac_sen['type_structure'].replace(mapping_types)

#  Standardisation régions
mapping_regions_facilities = {
    'Kaokack'    : 'Kaolack',
    'Saintlouis' : 'Saint-Louis',
    'Sediou'     : 'Sedhiou',
    'Kedougou'   : 'Kedougou',
    'Thies'      : 'Thies',
}
df_fac_sen['region']  = df_fac_sen['region'].replace(mapping_regions_facilities)
df_fac_sen['gestion'] = df_fac_sen['gestion'].fillna('Non renseigne')
df_fac_sen['pays']    = 'Senegal'

print(f"   {len(df_fac_sen)} structures → clean_health_facilities_senegal.csv")
print(f"   Types de structures :")
for t in sorted(df_fac_sen['type_structure'].unique()):
    print(f"    → {t}")

df_fac_sen.to_csv(
    os.path.join(STAGING_DATA_PATH, "clean_health_facilities_senegal.csv"),
    index=False, encoding='utf-8'
)

# =============================================================
# BILAN
# =============================================================
print("\n" + "="*60)
print("  BILAN DU NETTOYAGE")
print("="*60)
print(f"   clean_malaria_indicators.csv    → {len(df_malaria)} lignes")
print(f"   clean_malaria_subnational.csv   → {len(df_sub)} lignes")
print(f"   clean_health_malaria.csv        → {len(df_health_malaria)} lignes")
print(f"   clean_health_facilities_senegal → {len(df_fac_sen)} lignes")
print("\n Nettoyage termine !")
