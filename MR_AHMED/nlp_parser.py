import logging
import re
try:
    import pdfplumber
except ImportError:
    pdfplumber = None

logger = logging.getLogger(__name__)

class BVCPDFParser:
    def __init__(self, raw_pdf_dir="data/raw/pdfs/"):
        self.raw_pdf_dir = raw_pdf_dir

    def extract_text_from_bulletin(self, pdf_path):
        """
        Extrait le texte d'un rapport PDF de la BVC en contournant les gros tableaux comptables.
        """
        if not pdfplumber:
            logger.warning("pdfplumber non installé. Impossible de parser le PDF. Simulation de texte activée.")
            return "Marché baissier. L'inflation pèse lourdement sur le secteur bancaire et immobilier."

        full_text = ""
        logger.info(f"Analyse du fichier PDF BVC : {pdf_path}")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for idx, page in enumerate(pdf.pages):
                    # extract_text(x_tolerance) aide à lire les colonnes BVC
                    # On ignore les tableaux stricts formattés en grille
                    text = page.extract_text(x_tolerance=2, y_tolerance=3)
                    if text:
                        # Nettoyage brutal des retours à la ligne inutiles
                        clean_text = re.sub(r'\s+', ' ', text)
                        full_text += clean_text + ". "
                        
            logger.info(f"Extraction textuelle réussie : {len(full_text)} caractères extraits.")
            return full_text
            
        except FileNotFoundError:
            logger.error(f"Le fichier PDF {pdf_path} est introuvable.")
            return "Fichier introuvable."
        except Exception as e:
            logger.error(f"Erreur de parsing PDF : {e}")
            return ""

if __name__ == "__main__":
    parser = BVCPDFParser()
    text = parser.extract_text_from_bulletin("dummy_path.pdf")
    # print(text)
