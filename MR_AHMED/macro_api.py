import logging
import pandas as pd
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class BAMDataExtractor:
    def __init__(self, api_key=None, raw_data_path="data/raw/"):
        """
        Extracteur de données Macro depuis le portail API REST de Bank Al-Maghrib.
        Nécessite une API Key (Ocp-Apim-Subscription-Key).
        """
        self.api_key = api_key
        self.raw_data_path = raw_data_path
        self.base_url = "https://bkam.opendatasoft.com/api/explore/v2.1/catalog/datasets" # URL ou Endpoint typique OpenData (A ajuster au portail dev BAM)
        # BAM utilise souvent Azure API Management: https://api.centralbankofmorocco.ma/
        self.azure_base_url = "https://api.centralbankofmorocco.ma"

    def _make_request(self, endpoint, params=None):
        if not self.api_key or self.api_key == "YOUR_BAM_API_KEY":
            logger.warning("Clé API BAM manquante ou invalide. Mode Simulation activé.")
            return None
            
        headers = {
            "Ocp-Apim-Subscription-Key": self.api_key,
            "Accept": "application/json"
        }
        
        try:
            url = f"{self.azure_base_url}/{endpoint}"
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Erreur lors de la requête BAM ({endpoint}) : {e}")
            return None

    def fetch_bdt_curve(self, start_date, end_date):
        """
        API: "Courbe des Taux BDT - Version1"
        Récupère la courbe des Bons du Trésor marocain.
        """
        logger.info("Extraction de la Courbe des Taux BDT...")
        # Exemple d'endpoint type (doit matcher la documentation précise du Helpdesk)
        # endpoint = "bdt/v1/courbe"
        # data = self._make_request(endpoint, {"dateDebut": start_date, "dateFin": end_date})
        
        # Pour le Dry-Run sans clé :
        if not self.api_key or self.api_key == "YOUR_BAM_API_KEY":
            import os
            file_path = f"{self.raw_data_path}bam_bdt.parquet"
            if os.path.exists(file_path):
                return pd.read_parquet(file_path)
                
            dates = pd.date_range(start_date, end_date, freq='B')
            import numpy as np
            df = pd.DataFrame({
                'Date': dates,
                'Taux_BDT_52S': np.linspace(2.5, 3.0, len(dates)) + np.random.normal(0, 0.05, len(dates)),
                'Taux_BDT_10A': np.linspace(3.5, 4.0, len(dates)) + np.random.normal(0, 0.05, len(dates))
            })
            os.makedirs(self.raw_data_path, exist_ok=True)
            df.to_parquet(file_path)
            return df
            
        # Logique réelle de parsing du JSON
        # df = pd.DataFrame(data['resultats'])
        # df['Date'] = pd.to_datetime(df['date'])
        # return df
        return pd.DataFrame()

    def fetch_exchange_rates(self, start_date, end_date):
        """
        API: "Cours de change - Version1"
        Ex: USD/MAD ou EUR/MAD.
        """
        logger.info("Extraction des Cours de change BAM...")
        if not self.api_key or self.api_key == "YOUR_BAM_API_KEY":
            import os
            file_path = f"{self.raw_data_path}bam_fx.parquet"
            if os.path.exists(file_path):
                return pd.read_parquet(file_path)
                
            dates = pd.date_range(start_date, end_date, freq='B')
            import numpy as np
            df = pd.DataFrame({
                'Date': dates,
                'USD_MAD': np.random.normal(10.0, 0.2, len(dates)),
                'EUR_MAD': np.random.normal(10.9, 0.2, len(dates))
            })
            os.makedirs(self.raw_data_path, exist_ok=True)
            df.to_parquet(file_path)
            return df
        return pd.DataFrame()

    def fetch_macro_indicators(self, start_date, end_date):
        """
        Orchestre la récupération de toutes les données Bank Al-Maghrib.
        """
        df_bdt = self.fetch_bdt_curve(start_date, end_date)
        df_fx = self.fetch_exchange_rates(start_date, end_date)
        
        if df_bdt is not None and df_fx is not None:
            # On met Date en index avant le merge
            if 'Date' in df_bdt.columns: df_bdt.set_index('Date', inplace=True)
            if 'Date' in df_fx.columns: df_fx.set_index('Date', inplace=True)
            
            df_macro = df_bdt.join(df_fx, how='outer').ffill()
            df_macro.reset_index(inplace=True)
            
            import os
            os.makedirs(self.raw_data_path, exist_ok=True)
            file_path = f"{self.raw_data_path}bam_macro.parquet"
            df_macro.to_parquet(file_path)
            logger.info("Données Bank Al-Maghrib compilées avec succès.")
            return df_macro
            
        return pd.DataFrame()

if __name__ == "__main__":
    # Test
    extractor = BAMDataExtractor(api_key="YOUR_BAM_API_KEY")
    start = datetime.now() - timedelta(days=30)
    end = datetime.now()
    df = extractor.fetch_macro_indicators(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    print(df.head())
