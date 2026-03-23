import re
import pandas as pd
from pathlib import Path
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import TableItem

# 1. Configuration des dossiers
INPUT_DIR = Path("./RS/resumes_seance_2023")
OUTPUT_DIR = Path("./RS/extractions_csv_indices/2023")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Configuration Docling v2
pipeline_options = PdfPipelineOptions()
pipeline_options.do_table_structure = True
pipeline_options.do_ocr = True 

format_options = {InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
converter = DocumentConverter(format_options=format_options)

def clean_percentage(value):
    """Nettoie les valeurs boursières et gère les pourcentages."""
    if value is None or pd.isna(value): return 0.0
    s = str(value).replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = re.sub(r'[^\d.\-%]', '', s) 
    try:
        if "%" in s:
            return float(s.replace("%", "")) / 100
        return float(s)
    except:
        return 0.0

def main():
    pdf_files = list(INPUT_DIR.glob("*.pdf"))
    print(f"🚀 Extraction des indices depuis {len(pdf_files)} fichiers...")

    for i, pdf_path in enumerate(pdf_files):
        print(f"[{i+1}/{len(pdf_files)}] Fichier : {pdf_path.name}")
        try:
            result = converter.convert(pdf_path)
            
            # On cherche le premier tableau qui contient "MASI"
            for item, level in result.document.iterate_items():
                if isinstance(item, TableItem):
                    df = item.export_to_dataframe(result.document)
                    
                    # Détection : Le tableau des indices contient généralement "MASI" 
                    # dans ses cellules ou ses en-têtes
                    content_str = df.to_string().upper()
                    if "MASI" in content_str:
                        # Nettoyage : On transpose souvent car les indices sont en colonnes dans le PDF
                        # mais on les veut en lignes (ou vice-versa) pour le ML
                        
                        # Ajout de la date extraite du nom du fichier
                        date_match = re.search(r"(\d{8})", pdf_path.name)
                        current_date = date_match.group(1) if date_match else "Inconnue"
                        
                        # Sauvegarde brute pour inspection (Format ML)
                        out_path = OUTPUT_DIR / f"indices_{current_date}.csv"
                        
                        # Application du nettoyage sur les colonnes numériques
                        # (On saute la colonne des noms d'indices)
                        for col in df.columns[1:]:
                            df[col] = df[col].apply(clean_percentage)
                            
                        df["Date_Rapport"] = current_date
                        df.to_csv(out_path, index=False, encoding='utf-8-sig')
                        print(f"   ✅ Indices extraits dans : {out_path.name}")
                        break # On ne prend que le premier tableau d'indices trouvé
                        
        except Exception as e:
            print(f"   ❌ Erreur : {e}")

if __name__ == "__main__":
    main()