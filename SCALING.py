import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

# Chargement
df = pd.read_excel('ACTIONS_CLEAN1.xlsx')
df['Séance'] = pd.to_datetime(df['Séance'])
df = df.sort_values(['Instrument', 'Séance'])

# Sélection des colonnes numériques à normaliser (on exclut la date, l'instrument et les ID_...)
cols_to_scale = ['Ouverture', 'Dernier Cours', '+haut du jour', '+bas du jour', 
                 'Nombre de titres échangés', 'Volume des échanges', 'Capitalisation', 
                 'PIB', 'Taux directeur', 'Inflation', 'Chomage']

scaler = MinMaxScaler()
df[cols_to_scale] = scaler.fit_transform(df[cols_to_scale])

df.to_excel('ACTIONS_SCALED.xlsx', index=False)