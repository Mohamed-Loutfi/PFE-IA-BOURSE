import pandas as pd
import logging
import time
from datetime import datetime, timedelta

# Import robuste avec fallback
try:
    import BVCscrap as bvc
except ImportError:
    bvc = None

logger = logging.getLogger(__name__)

class BVCDataExtractor:
    def __init__(self, raw_data_path="data/raw/"):
        self.raw_data_path = raw_data_path
        self.masi20_tickers = [
            "ATW", "BCP", "BOA", "CIH", "CMA", "COS", "CRS", 
            "ADH", "IAM", "LHM", "MNG", "MSA", "MUT", "SID", 
            "SNA", "TQA"
        ] # Liste non-exhaustive des symboles liquides de la BVC

    def fetch_masi20_stocks(self, start_date, end_date):
        """
        Récupère l'historique complet des actions du MASI 20.
        """
        if not bvc:
            logger.warning("BVCscrap n'est pas trouvé dans cet environnement Python. Veillez à exécuter le main avec l'interpréteur où il est installé (uv / venv).")
            logger.info("Simulation d'un DataFrame de test BVC pour poursuivre la pipeline de build.")
            dates = pd.date_range(start_date, end_date, freq='B')
            import numpy as np
            return pd.DataFrame({
                'Date': dates,
                'Open': np.random.normal(100, 5, len(dates)),
                'High': np.random.normal(105, 5, len(dates)),
                'Low': np.random.normal(95, 5, len(dates)),
                'Close': np.random.normal(100, 5, len(dates)),
                'Volume': np.random.randint(100, 10000, len(dates)),
                'Ticker': ['SIM_STOCK'] * len(dates)
            })
            
        logger.info(f"Début de l'extraction BVC de {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}")
        all_data = []
        
        # MAPPING STRICT pour Pandas_TA : BVCscrap renvoie souvent en français
        rename_map = {
            'Date': 'Date',
            'Valeur': 'Close',      # Ou Dernier Cours -> Close
            'Ouverture': 'Open',
            'Plus Haut': 'High',
            'Plus Bas': 'Low',
            'Volume': 'Volume'
        }
        
        for ticker in self.masi20_tickers:
            try:
                logger.info(f"Scraping de {ticker}...")
                # Appel supposé de l'API BVCscrap (la signature exacte dépend de leur doc)
                df = bvc.loadata(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                if df is not None and not df.empty:
                    df['Ticker'] = ticker
                    # Tentative de nettoyage des colonnes (si les noms en Français sont renvoyés)
                    df.rename(columns=rename_map, inplace=True)
                    # S'assurer qu'on caste en FLOAT pour éviter les TypeError PyTorch plus tard
                    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                        if col in df.columns:
                            try:
                                # Nettoyage des chaînes type "1 200,50" si scraping salit
                                if df[col].dtype == object:
                                    df[col] = df[col].astype(str).str.replace(' ', '').str.replace(',', '.').astype(float)
                                else:
                                    df[col] = df[col].astype(float)
                            except:
                                pass
                    all_data.append(df)
                time.sleep(1) # Tolérance anti-ban
            except Exception as e:
                logger.error(f"Erreur lors de l'extraction de {ticker} : {e}")
                
        if not all_data:
            logger.error("Aucune donnée BVC récupérée.")
            return None
            
        dataset = pd.concat(all_data)
        file_path = f"{self.raw_data_path}masi20.parquet"
        dataset.to_parquet(file_path)
        logger.info(f"Données historiques BVC sauvegardées dans {file_path}")
        return dataset
        
if __name__ == "__main__":
    extractor = BVCDataExtractor()
    df = extractor.fetch_masi20_stocks(
        start_date=datetime.now() - timedelta(days=365*5), 
        end_date=datetime.now()
    )
    if df is not None:
        print(df.head())
