import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class HCPDataExtractor:
    def __init__(self, ckan_api_url="https://opendata.hcp.ma/api/3/action/datastore_search", raw_data_path="data/raw/"):
        """
        Extracteur de données socio-économiques via l'API CKAN du Haut-Commissariat au Plan (HCP).
        """
        self.api_url = ckan_api_url
        self.raw_data_path = raw_data_path

    def _fetch_ckan_resource(self, resource_id, limit=5000):
        """
        Méthode générique pour extraire les données d'un dataset CKAN spécifique.
        """
        if not resource_id or resource_id == "YOUR_CKAN_RESOURCE_ID":
            return None
            
        try:
            params = {'resource_id': resource_id, 'limit': limit}
            response = requests.get(self.api_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if data.get('success'):
                records = data['result']['records']
                return pd.DataFrame(records)
            else:
                logger.error(f"Échec de la requête CKAN HCP (ID: {resource_id}): {data.get('error')}")
                return None
        except Exception as e:
            logger.error(f"Erreur HTTP lors de l'accès au portail HCP: {e}")
            return None

    def fetch_macro_socio(self, start_date, end_date, resource_id_inflation=None, resource_id_unemployment=None):
        """
        Rapatrie les séries temporelles d'inflation (IPC) et de chômage.
        Si les vrais resource_ids ne sont pas fournis, génère des données simulées réalistes pour le Maroc.
        """
        logger.info("Tentative de récupération des données Macro HCP (Inflation, Chômage)...")
        
        df_inf = self._fetch_ckan_resource(resource_id_inflation)
        df_unemp = self._fetch_ckan_resource(resource_id_unemployment)
        
        # --- Mode Simulation (Fallback Pipeline) ---
        if df_inf is None or df_unemp is None:
            logger.warning("Ressources CKAN HCP non configurées ou inaccessibles. Simulation des indices macro-économiques annuels.")
            import numpy as np
            import os
            
            file_path = f"{self.raw_data_path}hcp_macro.parquet"
            if os.path.exists(file_path):
                logger.info("Chargement du dataset HCP simulé statique.")
                return pd.read_parquet(file_path)
                
            dates = pd.date_range(start_date, end_date, freq='ME') # 'ME' pour Month End
            
            df_hcp = pd.DataFrame({
                'Date': dates,
                'HCP_Inflation_MoM': np.random.normal(0.015, 0.005, len(dates)), # ~1.5% mensuel (Très simulé)
                'HCP_Chomage_Rate': np.linspace(11.0, 13.5, len(dates)) + np.random.normal(0, 0.2, len(dates)) # Chômage tendanciel
            })
            
            # Sauvegarder pour stabilité ML
            os.makedirs(self.raw_data_path, exist_ok=True)
            df_hcp.to_parquet(file_path)
            
            logger.info("Vecteurs macro-économiques HCP simulés et sauvegardés avec succès.")
            return df_hcp
            
        # --- Mode Réel (TODO avec les vrais IDs) ---
        # Le parsing dépendra de la structure exacte des colonnes des fichiers HCP.
        # Exemple abstrait :
        # df_hcp = pd.merge(df_inf, df_unemp, on="date_colonne")
        # return df_hcp
        
        return pd.DataFrame()

if __name__ == "__main__":
    hcp_api = HCPDataExtractor()
    start = datetime.now() - timedelta(days=365)
    end = datetime.now()
    df = hcp_api.fetch_macro_socio(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    print(df.head())
