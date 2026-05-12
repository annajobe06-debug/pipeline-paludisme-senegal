# =============================================================
# PROJET   : Pipeline Paludisme Sénégal
# ÉTAPE 6  : Machine Learning — Prévision Prophet
# MODÈLE   : Prédiction taux d'incidence 2025 → 2030
# Auteur   : Anna JOBE
# Date     : 2026
# =============================================================

import pandas as pd
import numpy as np
from prophet import Prophet
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import create_engine, text
import urllib
import os

OUTPUT_PATH = r"C:\Users\jobea\OneDrive\Documents\Pipeline_paludisme_senegal\07_outputs"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ── Connexion SQL Server ──────────────────────────────────────
print("\n" + "="*60)
print("  CONNEXION A SQL SERVER")
print("="*60)

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('DB_SERVER', 'ANNA\\SQLEXPRESS')};"
    f"DATABASE={os.getenv('DB_NAME', 'paludisme_senegal')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
    "TrustServerCertificate=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# ── Récupération des données ──────────────────────────────────
print("\n" + "="*60)
print("  EXTRACTION DES DONNÉES")
print("="*60)

query = """
SELECT
    d.annee,
    f.valeur AS taux_incidence
FROM fact_paludisme f
JOIN dim_date d
    ON f.id_date = d.id_date
JOIN dim_indicateur i
    ON f.id_indicateur = i.id_indicateur
WHERE i.code_indicateur = 'TAUX_INCIDENCE'
ORDER BY d.annee
"""

with engine.connect() as conn:
    df = pd.read_sql(text(query), conn)

print(f"   {len(df)} années extraites")
print(f"   Période : {df['annee'].min()} → {df['annee'].max()}")
print(f"   Taux min : {df['taux_incidence'].min():.2f}")
print(f"   Taux max : {df['taux_incidence'].max():.2f}")

# ── Préparation des données pour Prophet ─────────────────────
print("\n" + "="*60)
print("  PRÉPARATION POUR PROPHET")
print("="*60)

# Prophet nécessite colonnes 'ds' et 'y'
df_prophet = pd.DataFrame({
    'ds': pd.to_datetime(df['annee'].astype(str) + '-01-01'),
    'y' : df['taux_incidence']
})

print(f"   Format Prophet prêt")
print(df_prophet.head())

# ── Entraînement du modèle Prophet ───────────────────────────
print("\n" + "="*60)
print("  ENTRAÎNEMENT DU MODÈLE PROPHET")
print("="*60)

model = Prophet(
    yearly_seasonality      = False,
    weekly_seasonality      = False,
    daily_seasonality       = False,
    changepoint_prior_scale = 0.3,   # Contrôle la flexibilité
    interval_width          = 0.95   # Intervalle confiance 95%
)

model.fit(df_prophet)
print("   Modèle entraîné avec succès !")

# ── Prévision 2025 → 2030 ─────────────────────────────────────
print("\n" + "="*60)
print("  PRÉVISION 2025 → 2030")
print("="*60)

# Créer les dates futures
future = model.make_future_dataframe(periods=7, freq='YE')
forecast = model.predict(future)

# Empêche les valeurs négatives
forecast['yhat']       = forecast['yhat'].clip(lower=0)
forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)

# Filtrer les prévisions futures
previsions = forecast[forecast['ds'].dt.year >= 2025][[
    'ds', 'yhat', 'yhat_lower', 'yhat_upper'
]].copy()

previsions['annee']       = previsions['ds'].dt.year
previsions['prediction']  = previsions['yhat'].round(2)
previsions['borne_basse'] = previsions['yhat_lower'].round(2)
previsions['borne_haute'] = previsions['yhat_upper'].round(2)

print("\n  Prévisions taux d'incidence :")
print("  " + "="*50)
for _, row in previsions.iterrows():
    print(f"  {int(row['annee'])} → {row['prediction']:.2f} "
          f"[{row['borne_basse']:.2f} - {row['borne_haute']:.2f}]")

# ── Objectif OMS 2030 ─────────────────────────────────────────
taux_2000      = df[df['annee'] == 2000]['taux_incidence'].values[0]
objectif_2030  = taux_2000 * 0.10  # Réduction de 90%
prediction_2030 = previsions[previsions['annee'] == 2030]['prediction'].values[0]
ecart          = prediction_2030 - objectif_2030

print(f"\n  Taux référence 2000   : {taux_2000:.2f} /1000")
print(f"  Objectif OMS 2030    : {objectif_2030:.2f} /1000 (-90%)")
print(f"  Prédiction 2030      : {prediction_2030:.2f} /1000")
print(f"  Écart objectif OMS   : +{ecart:.2f} /1000")

# ── Visualisation améliorée ───────────────────────────────────
fig, ax = plt.subplots(figsize=(16, 8))

# Données historiques
ax.plot(
    df['annee'], df['taux_incidence'],
    color='#1B5E20', linewidth=2.5,
    marker='o', markersize=5,
    label='Données historiques (OMS)',
    zorder=5
)

# Prévisions
annees_futures = previsions['annee'].values
ax.plot(
    annees_futures, previsions['prediction'],
    color='#43A047', linewidth=2.5,
    marker='s', markersize=6,
    linestyle='--',
    label='Prévision Prophet 2025-2030',
    zorder=5
)

# Intervalle de confiance — plus discret
ax.fill_between(
    annees_futures,
    previsions['borne_basse'],
    previsions['borne_haute'],
    alpha=0.15, color='#43A047',
    label='Intervalle de confiance 95%'
)

# Seuil critique OMS
ax.axhline(
    y=50, color='#E67E22',
    linestyle=':', linewidth=1.5,
    label='Seuil critique OMS : 50/1000'
)

# Objectif OMS 2030
ax.axhline(
    y=objectif_2030, color='#C0392B',
    linestyle=':', linewidth=1.5,
    label=f'Objectif OMS 2030 : {objectif_2030:.1f}/1000 (-90%)'
)

# Ligne séparation
ax.axvline(
    x=2024.5, color='#7F8C8D',
    linestyle='--', linewidth=1,
    alpha=0.7
)

# Zone prévision — fond légèrement coloré
ax.axvspan(2024.5, 2031, alpha=0.05, color='#43A047')

# Texte séparation
ax.text(
    2024.7, 220,
    'Prévision →',
    fontsize=9, color='#7F8C8D',
    style='italic'
)

# Annotation prédiction 2030 — repositionnée
ax.annotate(
    f'2030 : {prediction_2030:.1f}/1000',
    xy=(2030, prediction_2030),
    xytext=(2027.5, prediction_2030 + 30),
    fontsize=10, color='#1B5E20',
    fontweight='bold',
    arrowprops=dict(
        arrowstyle='->',
        color='#1B5E20',
        lw=1.5
    )
)

# Annotation objectif OMS — repositionnée
ax.annotate(
    f'Objectif OMS : {objectif_2030:.1f}/1000',
    xy=(2030, objectif_2030),
    xytext=(2026, objectif_2030 - 25),
    fontsize=10, color='#C0392B',
    fontweight='bold',
    arrowprops=dict(
        arrowstyle='->',
        color='#C0392B',
        lw=1.5
    )
)

# Annotation écart
ax.annotate(
    f'Écart : +{ecart:.1f}/1000',
    xy=(2030, (prediction_2030 + objectif_2030) / 2),
    xytext=(2025.5, (prediction_2030 + objectif_2030) / 2),
    fontsize=9, color='#7F8C8D',
    style='italic'
)

# Mise en forme
ax.set_title(
    "Prévision du Taux d'Incidence du Paludisme au Sénégal\n"
    "Modèle Prophet — Horizon 2030 | Objectif OMS End Malaria",
    fontsize=14, fontweight='bold',
    color='#1B5E20', pad=20
)
ax.set_xlabel("Année", fontsize=12, labelpad=10)
ax.set_ylabel("Taux d'incidence (/1000 habitants)", fontsize=12, labelpad=10)

# Légende — positionnée en haut à droite sans chevaucher
ax.legend(
    loc='upper right',
    fontsize=9,
    framealpha=0.9,
    edgecolor='#BDC3C7'
)

ax.set_facecolor('#F9FBE7')
fig.patch.set_facecolor('#FFFFFF')
ax.grid(True, alpha=0.2, color='#BDC3C7', linestyle='-')
ax.set_xlim(1999, 2031.5)
ax.set_ylim(-10, 250)

# Années sur axe X bien espacées
ax.set_xticks(range(2000, 2031, 2))
ax.tick_params(axis='x', rotation=45)

plt.tight_layout(pad=2.0)

# Sauvegarde
output_file = os.path.join(OUTPUT_PATH, "prophet_prediction_incidence.png")
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"   Graphique sauvegardé : prophet_prediction_incidence.png")
plt.show()

# ── Bilan final ───────────────────────────────────────────────
print("\n" + "="*60)
print("  BILAN FINAL")
print("="*60)
print(f"   Modèle Prophet entraîné sur {len(df)} années")
print(f"   Prévisions générées : 2025 → 2030")
print(f"   Taux prédit 2030 : {prediction_2030:.2f} /1000")
print(f"   Objectif OMS 2030 : {objectif_2030:.2f} /1000")
print(f"   Écart objectif : +{ecart:.2f} /1000")
print(f"\n    Conclusion : Le Sénégal ne sera pas")
print(f"     en mesure d'atteindre l'objectif OMS")
print(f"     de -90% d'incidence d'ici 2030")
print(f"     sans intervention supplémentaire !")
