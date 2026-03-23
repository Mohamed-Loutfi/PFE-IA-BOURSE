import pandas as pd
import glob

# Trouver tous les fichiers CSV des actions (ex: ceux qui commencent par une majuscule ou un pattern précis)
fichiers_actions = glob.glob("./actions_MASI20/*.xlsx") 
# On exclut les fichiers macro pour ne pas les mélanger
fichiers_actions = [f for f in fichiers_actions if "PIB" not in f and "CHOMAGE" not in f]

liste_df = []

for f in fichiers_actions:
    df_temp = pd.read_excel(f)
    # On s'assure que la date est au bon format
    df_temp['Séance'] = pd.to_datetime(df_temp['Séance'], format='%d/%m/%Y')
    liste_df.append(df_temp)

# Fusion verticale : on empile les actions les unes sous les autres
df_bourse_total = pd.concat(liste_df, ignore_index=True)
# Sauvegarde le résultat dans un nouveau fichier sur votre disque
df_bourse_total.to_excel("Actions_fusionnees.xlsx", index=False)

df_pib=pd.read_excel('PIB.xlsx',skiprows=4)
df_pib = df_pib.dropna(subset=['Date de publication'])
df_pib['Date de publication'] = pd.to_datetime(df_pib['Date de publication'],dayfirst=True)
df_pib = df_pib.sort_values('Date de publication')

df_macro = pd.read_excel('TX_CHOMAGE.xlsx', skiprows=1)
df_macro = df_macro.dropna(subset=['Date de publication'])
df_macro['Date de publication'] = pd.to_datetime(df_macro['Date de publication'],dayfirst=True)
df_macro = df_macro.sort_values('Date de publication')

df_directeur = pd.read_excel('TX_DIRECTEUR.xlsx', skiprows=1)
df_directeur = df_directeur.dropna(subset=['Date de publication'])
df_directeur['Date de publication'] = pd.to_datetime(df_directeur['Date de publication'],dayfirst=True)
df_directeur = df_directeur.sort_values('Date de publication')

df_bourse_total = df_bourse_total.sort_values('Séance')
df_final = pd.merge_asof(df_bourse_total,
                         df_pib[['Date de publication', 'PIB']],
                         left_on='Séance',
                         right_on='Date de publication',
                            direction='backward')
df_final=df_final.rename(columns={'Date de publication': 'Date_pub_pib'})
df_final = pd.merge_asof(df_final,
                            df_directeur[['Date de publication', 'Taux directeur']],
                            left_on='Séance',
                            right_on='Date de publication',
                                direction='backward',)
df_final=df_final.rename(columns={'Date de publication': 'Date_pub_directeur'})
df_final = pd.merge_asof(df_final,
                            df_macro[['Date de publication', 'Inflation', 'Chomage']],
                            left_on='Séance',
                            right_on='Date de publication',
                                direction='backward',
                                suffixes=('_pib','_directeur', '_macro'))
df_final=df_final.rename(columns={'Date de publication': 'Date_pub_macro'})
df_final = df_final.drop(columns=['Date_pub_pib', 'Date_pub_directeur', 'Date_pub_macro'])
df_final.to_excel("Bourse_Macro_Fusionnees1.xlsx", index=False)