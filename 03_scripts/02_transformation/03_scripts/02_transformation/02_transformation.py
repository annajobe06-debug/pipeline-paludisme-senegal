# =============================================================
# PROJET   : Pipeline Paludisme Sénégal
# ÉTAPE 2  : Transformation — Format long → Format large
# VERSION  : 3.0
# Auteur   : Anna JOBE
# Date     : 2026
# =============================================================

import pandas as pd
import os

STAGING_PATH = r"C:\Users\jobea\OneDrive\Documents\Pipeline_paludisme_senegal\02_staging"

# =============================================================
# TRANSFORMATION 1 — malaria_indicators
# =============================================================
print("\n" + "="*60)
print("  TRANSFORMATION 1 : malaria_indicators")
print("="*60)

df_ind = pd.read_csv(os.path.join(STAGING_PATH, "clean_malaria_indicators.csv"))

df_ind['indicateur_nom'] = df_ind['indicateur_nom'].str.strip()
df_ind['indicateur_nom'] = df_ind['indicateur_nom'].str.replace(r'\s+', ' ', regex=True)

mapping = {
    "Number of confirmed malaria cases"                          : "nb_cas_confirmes",
    "Number of presumed malaria cases"                           : "nb_cas_presumes",
    "Total number of malaria cases (presumed + confirmed cases)" : "nb_cas_total",
    "Estimated malaria incidence (per 1000 population at risk)"  : "taux_incidence",
    "Estimated malaria mortality rate (per 100 000 population)"  : "taux_mortalite",
}

df_ind_filtre            = df_ind[df_ind["indicateur_nom"].isin(mapping.keys())].copy()
df_ind_filtre["colonne"] = df_ind_filtre["indicateur_nom"].map(mapping)

print(f"  nb_cas_total : {len(df_ind_filtre[df_ind_filtre['colonne'] == 'nb_cas_total'])} lignes")

df_ind_pivot = df_ind_filtre.pivot_table(
    index   = "annee",
    columns = "colonne",
    values  = "valeur_numerique",
    aggfunc = "first"
).reset_index()

df_incidence = df_ind[
    df_ind["indicateur_nom"] == "Estimated malaria incidence (per 1000 population at risk)"
][["annee", "valeur_basse", "valeur_haute"]]

df_ind_pivot                  = df_ind_pivot.merge(df_incidence, on="annee", how="left")
df_ind_pivot["niveau"]        = "national"
df_ind_pivot["pays"]          = "Senegal"
df_ind_pivot["source_donnee"] = "OMS / HDX"

df_ind_pivot.to_csv(
    os.path.join(STAGING_PATH, "transformed_malaria_national.csv"), index=False
)
print(f"   {len(df_ind_pivot)} lignes → transformed_malaria_national.csv")
print(f"   Colonnes : {list(df_ind_pivot.columns)}")

# =============================================================
# TRANSFORMATION 2 — malaria_subnational
# =============================================================
print("\n" + "="*60)
print("  TRANSFORMATION 2 : malaria_subnational")
print("="*60)

df_sub = pd.read_csv(os.path.join(STAGING_PATH, "clean_malaria_subnational.csv"))

df_sub_final = df_sub[[
    "region", "annee", "prevalence_pct",
    "population_enquetee", "intervalle_bas", "intervalle_haut"
]].copy()

df_sub_final.rename(columns={
    "prevalence_pct"  : "prevalence_rdt",
    "intervalle_bas"  : "valeur_basse",
    "intervalle_haut" : "valeur_haute"
}, inplace=True)

#  Filtre : garder uniquement les vraies prévalences (0 à 100%)
avant = len(df_sub_final)
df_sub_final = df_sub_final[
    (df_sub_final['prevalence_rdt'] >= 0) &
    (df_sub_final['prevalence_rdt'] <= 100)
].copy()
print(f"  Lignes filtrées (valeurs aberrantes) : {avant - len(df_sub_final)}")

df_sub_final["source_donnee"] = "DHS / HDX"

df_sub_final.to_csv(
    os.path.join(STAGING_PATH, "transformed_malaria_regional.csv"), index=False
)
print(f"   {len(df_sub_final)} lignes → transformed_malaria_regional.csv")

# =============================================================
# TRANSFORMATION 3 — health_malaria (prévention)
# =============================================================
print("\n" + "="*60)
print("  TRANSFORMATION 3 : health_malaria (prevention)")
print("="*60)

df_health = pd.read_csv(os.path.join(STAGING_PATH, "clean_health_malaria.csv"))

df_health['indicateur_nom'] = df_health['indicateur_nom'].str.strip()
df_health['indicateur_nom'] = df_health['indicateur_nom'].str.replace(r'\s+', ' ', regex=True)

mapping_health = {
    "Children with fever receiving antimalarial drugs (% of children under age 5 with fever)" : "taux_traitement_enfants",
    "Intermittent preventive treatment (IPT) of malaria in pregnancy (% of pregnant women)"   : "taux_tpi_grossesse",
}

df_health_filtre            = df_health[df_health["indicateur_nom"].isin(mapping_health.keys())].copy()
df_health_filtre["colonne"] = df_health_filtre["indicateur_nom"].map(mapping_health)

df_health_pivot = df_health_filtre.pivot_table(
    index   = "annee",
    columns = "colonne",
    values  = "valeur",
    aggfunc = "first"
).reset_index()

df_health_pivot["source_donnee"] = "World Bank / HDX"

df_health_pivot.to_csv(
    os.path.join(STAGING_PATH, "transformed_prevention.csv"), index=False
)
print(f"   {len(df_health_pivot)} lignes → transformed_prevention.csv")
print(f"   Colonnes : {list(df_health_pivot.columns)}")

# =============================================================
# TRANSFORMATION 4 — structures par région
# =============================================================
print("\n" + "="*60)
print("  TRANSFORMATION 4 : structures sanitaires par region")
print("="*60)

df_fac    = pd.read_csv(os.path.join(STAGING_PATH, "clean_health_facilities_senegal.csv"))
df_struct = df_fac.groupby("region").size().reset_index()
df_struct.columns = ["region", "nb_structures_sanitaires"]

for _, row in df_struct.iterrows():
    print(f"    → {row['region']:<20} : {row['nb_structures_sanitaires']} structures")

df_struct.to_csv(
    os.path.join(STAGING_PATH, "transformed_structures_par_region.csv"), index=False
)
print(f"   {len(df_struct)} regions → transformed_structures_par_region.csv")

# =============================================================
# BILAN
# =============================================================
print("\n" + "="*60)
print("  BILAN DE LA TRANSFORMATION")
print("="*60)
print(f"   transformed_malaria_national.csv      → {len(df_ind_pivot)} lignes")
print(f"   transformed_malaria_regional.csv      → {len(df_sub_final)} lignes")
print(f"   transformed_prevention.csv            → {len(df_health_pivot)} lignes")
print(f"   transformed_structures_par_region.csv → {len(df_struct)} lignes")
print("\n Transformation terminee. Pret pour le chargement SQL !")
