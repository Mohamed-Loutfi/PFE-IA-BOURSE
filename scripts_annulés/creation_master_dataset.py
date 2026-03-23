import pandas as pd
from pathlib import Path

# Configuration des dossiers
PATH_ACTIONS = Path("./RS/extractions_csv_Cours")
PATH_INDICES = Path("./RS/extractions_csv_indices")
OUTPUT_FILE = "BVC_Master_Dataset.csv"

def build_master():
    print("Début de la fusion globale...")

    # ==============================
    # PARTIE A : REGROUPEMENT ACTIONS
    # ==============================
    print("Step 1: Regroupement des fichiers actions...")

    actions_files = list(PATH_ACTIONS.glob("**/*.csv"))
    if not actions_files:
        raise ValueError("Aucun fichier action trouvé.")

    all_actions = []
    for f in actions_files:
        df = pd.read_csv(f)
        all_actions.append(df)

    df_actions_total = pd.concat(all_actions, ignore_index=True)

    # Conversion propre en datetime (format AAAAMMJJ)
    df_actions_total['Date_Rapport'] = pd.to_datetime(
        df_actions_total['Date_Rapport'],
        format='%Y-%m-%d',
        errors='coerce'
    )

    # ==============================
    # PARTIE B : REGROUPEMENT INDICES
    # ==============================
    print("Step 2: Regroupement des fichiers indices...")

    indices_files = list(PATH_INDICES.glob("**/*.csv"))
    if not indices_files:
        raise ValueError("Aucun fichier indice trouvé.")

    all_indices_processed = []

    for f in indices_files:
        df_idx = pd.read_csv(f)

        # On garde uniquement la ligne "Valeur"
        df_valeur = df_idx[
            df_idx['Indices'].str.contains('Valeur', case=False, na=False)
        ].copy()

        if df_valeur.empty:
            continue

        df_valeur = df_valeur.drop(columns=['Indices'])
        all_indices_processed.append(df_valeur)

    if not all_indices_processed:
        raise ValueError("Aucune ligne 'Valeur' trouvée dans les fichiers indices.")

    df_indices_total = pd.concat(all_indices_processed, ignore_index=True)

    # Conversion propre en datetime
    df_indices_total['Date_Rapport'] = pd.to_datetime(
        df_indices_total['Date_Rapport'],
        format='%Y%m%d',
        errors='coerce'
    )

    # ==============================
    # PARTIE C : FUSION FINALE
    # ==============================
    print("Step 3: Fusion finale...")

    master_df = pd.merge(
        df_actions_total,
        df_indices_total,
        on='Date_Rapport',
        how='left'
    )

    # ==============================
    # NETTOYAGE FINAL
    # ==============================

    # Suppression éventuelle des dates invalides
    master_df = master_df.dropna(subset=['Date_Rapport'])

    # Tri chronologique réel
    master_df = master_df.sort_values(by=['Action', 'Date_Rapport'])

    # Reset index propre
    master_df = master_df.reset_index(drop=True)

    # Sauvegarde
    master_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

    print(f"Terminé ! Fichier créé : {OUTPUT_FILE}")
    print(f"{len(master_df)} lignes")
    print(f"{master_df['Action'].nunique()} actions différentes")

if __name__ == "__main__":
    build_master()