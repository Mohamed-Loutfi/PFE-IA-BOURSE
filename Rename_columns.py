import pandas as pd

# Chargement du dernier fichier propre
df = pd.read_excel('ACTIONS_CLEAN1.xlsx')

# Dictionnaire de renommage
mapping = {
    'Séance': 'Date',
    'Ouverture': 'Open',
    '+haut du jour': 'High',
    '+bas du jour': 'Low',
    'Dernier Cours': 'Close',
    'Volume des échanges': 'Volume',
    'Cours ajusté': 'Adj_Close'
}

# Application du changement
df = df.rename(columns=mapping)

# Mise au format Datetime (indispensable pour l'analyse technique)
df['Date'] = pd.to_datetime(df['Date'])

# Sauvegarde
df.to_excel('Bourse_Standardisee1.xlsx', index=False)

print("Colonnes renommées :", df.columns.tolist())