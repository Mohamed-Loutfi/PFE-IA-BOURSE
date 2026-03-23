import pandas as pd

# Chargement du dataset fusionné
df = pd.read_excel('Bourse_Macro_Fusionnees1.xlsx')

# --- ÉTAPE 2 : NETTOYAGE ET IMPUTATION DES VALEURS NULLES ---
# On traite les colonnes numériques qui ont des "trous" (comme 'Nombre de contrats')
# On utilise la moyenne spécifique à chaque Ticker pour ne pas fausser les données

cols_numeriques = df.select_dtypes(include=['float64', 'int64']).columns

for col in cols_numeriques:
    # On remplit les cellules vides par la moyenne du groupe (Ticker)
    df[col] = df.groupby('Ticker')[col].transform(lambda x: x.fillna(x.mean()))

# Si certaines actions n'ont aucune donnée du tout pour une colonne, 
# on remplit le reste par la moyenne globale par sécurité
df = df.fillna(df.mean(numeric_only=True))

print("Étape 2 terminée : Valeurs nulles traitées.")

# --- ÉTAPE 3 : ENCODAGE CATÉGORIEL (ONE-HOT ENCODING) ---
# On transforme la colonne 'Ticker' (ex: AKT, IAM) en plusieurs colonnes de 0 et 1
# Cela permet au modèle de Deep Learning de distinguer les actions.

df_final = pd.get_dummies(df, columns=['Ticker'], prefix='ID')

# Note : On garde la colonne 'Instrument' ou 'Séance' pour la lisibilité, 
# mais elles devront être exclues ou transformées lors de l'entraînement.

print("Étape 3 terminée : Tickers convertis en vecteurs binaires (One-Hot).")

df_final.to_excel('Bourse_clean.xlsx', index=False)

# Affichage des premières lignes pour vérifier
print("\nStructure du fichier après étape 3 :")
print(df_final.head())


# Liste des colonnes de prix où le '0' est une erreur
cols_prix = ['Ouverture', 'Dernier Cours', '+haut du jour', '+bas du jour', 'Cours ajusté']

# 1. Remplacer les 0 par NaN pour pouvoir les 'remplir' intelligemment
df_final[cols_prix] = df_final[cols_prix].replace(0, pd.NA)

# 2. Trier par action et par date
df_final = df_final.sort_values(['Instrument', 'Séance'])

# 3. Appliquer le Forward Fill (par action)
# Si pas de prix aujourd'hui, on prend celui d'hier
df_final[cols_prix] = df_final.groupby('Instrument')[cols_prix].ffill()

# 4. Appliquer le Backward Fill pour les toutes premières lignes (si besoin)
df_final[cols_prix] = df_final.groupby('Instrument')[cols_prix].bfill()

# 5. Remplir les volumes manquants par 0 (ici le 0 est correct : pas d'échange)
df_final[['Nombre de titres échangés', 'Volume des échanges', 'Nombre de contrats']] = \
    df_final[['Nombre de titres échangés', 'Volume des échanges', 'Nombre de contrats']].fillna(0)

# Sauvegarde finale
df_final.to_excel('ACTIONS_CLEAN1.xlsx', index=False)