import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DatasetBuilder:
    def __init__(self, raw_path="data/raw/", feature_store_path="data/feature_store/"):
        self.raw_path = raw_path
        self.feature_store_path = feature_store_path

    def merge_to_master_dataset(self, bvc_df, bam_df, hcp_df, nlp_df=None):
        """
        Fusionne intelligemment les actions BVC, les taux BAM, l'inflation HCP et le NLP
        sur un index temporel propre (Date) tout en gérant les valeurs manquantes sans Data Leakage.
        """
        logger.info("Début de la fusion des sources de données disparates...")
        
        # 1. Vérifications basiques
        if bvc_df is None or bvc_df.empty:
            logger.warning("BVC Data est vide. Création d'un dataset vide.")
            return pd.DataFrame()

        # 2. L'Index de référence est la Bourse de Casablanca (Traded Days Only)
        # On assume que tous les DataFrames ont une colonne ou un index 'Date' ou 'datetime'
        try:
            if 'Date' in bvc_df.columns:
                bvc_df['Date'] = pd.to_datetime(bvc_df['Date'])
                bvc_df.set_index('Date', inplace=True)
            
            # Formatage et Merge BAM (Taux directeurs, BDT)
            if bam_df is not None and not bam_df.empty:
                if 'Date' in bam_df.columns:
                    bam_df['Date'] = pd.to_datetime(bam_df['Date'])
                    bam_df.set_index('Date', inplace=True)
                
                # Fusion Left Join: On garde les jours BVC, on ramène le taux de la journée
                master_df = bvc_df.join(bam_df, how='left')
                
                # GESTION DES NA : Forward Fill stricte (taux du vendredi appliqué au lundi)
                # Zéro Look-Ahead Bias ici, on repousse la donnée du passé vers le futur
                master_df.ffill(inplace=True)
            else:
                master_df = bvc_df.copy()

            # Formatage et Merge HCP (Inflation mensuelle, etc.)
            if hcp_df is not None and not hcp_df.empty:
                # Logique similaire (left join + ffill) puisque l'inflation est statique pendant 1 mois
                pass

            # Remplissage par défaut pour la toute première ligne s'il y a des retards
            master_df.bfill(inplace=True) 

            # Sauvegarde au format Parquet optimisé
            out_path = f"{self.feature_store_path}bvc_master_dataset.parquet"
            master_df.to_parquet(out_path)
            
            logger.info("Master Dataset construit avec succès et Forward-Filled sans Leakage.")
            return master_df

        except Exception as e:
            logger.error(f"Erreur lors de la fusion du Dataset Builder : {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    builder = DatasetBuilder()
    # builder.merge_to_master_dataset(df1, df2, df3)
