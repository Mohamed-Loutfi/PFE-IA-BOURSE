import pandas as pd
import numpy as np
import pandas_ta as ta

# 1. Chargement du Master Dataset
df = pd.read_csv('BVC_Master_Dataset.csv')

# 2. Dictionnaire de Mapping (Corrigé et Élargi)
ticker_map = {
    # MASI 20 (Cible principale)
    "ATTIJARIWAFA BANK": "ATW",
    "ITISSALAT AL-MAGHRIB": "IAM", "MAROC TELECOM": "IAM",
    "BANK OF AFRICA": "BOA",
    "BANQUE CENTRALE POPULAIRE": "BCP", "BCP": "BCP",
    "LAFARGEHOLCIM MAROC": "LHM", "LAFARGEHOLCIM": "LHM",
    "CIMENTS DU MAROC": "CMA",
    "MANAGEM": "MNG",
    "COSUMAR": "COS",
    "MARSA MAROC": "MSA", "SODEP-MARSA MAROC": "MSA",
    "DOUJA PROM ADDOHA": "ADH",
    "TAQA MOROCCO": "TQA", "TAQA": "TQA",
    "AKDITAL": "AKT",
    "CIH BANK": "CIH", "CIH": "CIH",
    "SONASID": "SID",
    "AFRIQUIA GAZ": "AFG",
    "LABEL VIE": "LBV", "LABEL": "LBV",
    "SOTHEMA": "SOT",
    "ALLIANCES": "ADI",
    "TOTALENERGIES MARKETING MAROC": "TQM", "TOTALENERGIES MARKETING": "TQM",
    "MUTANDIS": "MUT", "MUTANDIS SCA": "MUT",
    "SMI": "SMI",

    # Autres Actions (MASI Complet)
    "AFMA": "AFMA",
    "AFRIC INDUSTRIES SA": "AFRI",
    "AGMA": "AGMA",
    "ALUMINIUM DU MAROC": "ALM",
    "ARADEI CAPITAL": "ARD",
    "ATLANTASANAD": "ASL",
    "AUTO HALL": "ATH",
    "AUTO NEJMA": "NEJ",
    "BALIMA": "BAL",
    "BMCI": "BCI",
    "CARTIER SAADA": "CRS",
    "CDM": "CDM",
    "CFG BANK": "CFG",
    "COLORADO": "COL",
    "CTM": "CTM",
    "DARI COUSPATE": "DRI",
    "DELTA HOLDING": "DHO",
    "DISTY TECHNOLOGIES": "DYT",
    "DISWAY": "DWAY",
    "ENNAKL": "NKL",
    "EQDOM": "EQD",
    "FENIE BROSSETTE": "FBE",
    "HPS": "HPS",
    "IB MAROC.COM": "IBM",
    "IMMORENTE INVEST": "IMO",
    "INVOLYS": "INV",
    "JET CONTRACTORS": "JET",
    "LESIEUR CRISTAL": "LES",
    "M2M GROUP": "M2M",
    "MAGHREB OXYGENE": "MOX",
    "MAGHREBAIL": "MAB",
    "MAROC LEASING": "MLE",
    "MED PAPER": "PAP",
    "MICRODATA": "MIC",
    "MINIERE TOUISSIT": "CMT",
    "OULMES": "OUL",
    "PROMOPHARM S.A.": "PRO",
    "REBAB COMPANY": "REB",
    "RESIDENCES DAR SAADA": "RDS",
    "RISMA": "RIS",
    "SALAFIN": "SLF",
    "SANLAM MAROC": "SAH",
    "SNEP": "SNP",
    "STOKVIS NORD AFRIQUE": "STO",
    "STROC INDUSTRIE": "STR",
    "TGCC": "TGC",
    "TIMAR": "TIM",
    "UNIMER": "UMR",
    "WAFA ASSURANCE": "WAA", "WAFA": "WAA",
    "ZELLIDJA S.A": "ZEL", "ZELLIDJA": "ZEL"
}

# 3. Nettoyage et Mapping
df['Ticker'] = df['Action'].str.strip().map(ticker_map)
df_clean = df.dropna(subset=['Ticker']).copy()
df_clean['Date_Rapport'] = pd.to_datetime(df_clean['Date_Rapport'])
df_clean = df_clean.sort_values(['Ticker', 'Date_Rapport'])

# 4. Fonction de Feature Engineering
def apply_indicators(group):
    group['RSI'] = ta.rsi(group['Cours_Cloture'], length=14)
    macd = ta.macd(group['Cours_Cloture'])
    if macd is not None:
        group['MACD'] = macd['MACD_12_26_9']
    group['Log_Returns'] = np.log(group['Cours_Cloture'] / group['Cours_Cloture'].shift(1))
    group['time_idx'] = np.arange(len(group))
    return group

print("Calcul des indicateurs pour toutes les actions...")
all_enriched = df_clean.groupby('Ticker', group_keys=False).apply(apply_indicators)
all_enriched = all_enriched.dropna(subset=['MACD']) # Supprime le warm-up des 26 premiers jours

# 5. Création du fichier MASI 20 (Filtre strict)
masi20_tickers = ["ATW", "IAM", "BCP", "BOA", "LHM", "CMA", "MNG", "COS", "MSA", "ADH", "TQA", "AKT", "CIH", "SID", "AFG", "LBV", "SOT", "ADI", "TQM", "MUT"]
masi20_enriched = all_enriched[all_enriched['Ticker'].isin(masi20_tickers)].copy()

# 6. Sauvegarde des deux fichiers
all_enriched.to_csv('BVC_All_Actions_Enriched.csv', index=False, encoding='utf-8-sig')
masi20_enriched.to_csv('BVC_MASI20_Enriched.csv', index=False, encoding='utf-8-sig')

print(f"✅ Fichiers créés :")
print(f"- Toutes les actions : BVC_All_Actions_Enriched.csv ({len(all_enriched)} lignes)")
print(f"- Sociétés MASI 20 : BVC_MASI20_Enriched.csv ({len(masi20_enriched)} lignes)")