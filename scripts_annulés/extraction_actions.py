import re
import pandas as pd
from pathlib import Path
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import TableItem

# ==============================
# CONFIGURATION
# ==============================

INPUT_DIR = Path("./RS/resumes_seance_2025")
OUTPUT_DIR = Path("./RS/extractions_csv_Cours/2025")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

pipeline_options = PdfPipelineOptions()
pipeline_options.do_table_structure = True
pipeline_options.do_ocr = True
pipeline_options.table_structure_options.do_cell_matching = True

format_options = {
    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
}

converter = DocumentConverter(format_options=format_options)

# ==============================
# NETTOYAGE NUMÉRIQUE ROBUSTE
# ==============================

def clean_numeric(value):
    if value is None or pd.isna(value):
        return None

    s = str(value)

    # Correction erreurs OCR fréquentes
    s = s.replace("O", "0").replace("l", "1")

    s = s.replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = re.sub(r"[^\d.\-%]", "", s)

    if "%" in s:
        try:
            return float(s.replace("%", "")) / 100
        except:
            return None

    try:
        return float(s)
    except:
        return None


# ==============================
# EXTRACTION PRINCIPALE
# ==============================

def main():
    pdf_files = list(INPUT_DIR.glob("*.pdf"))
    print(f"Traitement de {len(pdf_files)} rapports boursiers...")

    for i, pdf_path in enumerate(pdf_files):
        print(f"[{i+1}/{len(pdf_files)}] Analyse : {pdf_path.name}")

        try:
            result = converter.convert(pdf_path)
            tables_segments = []

            for item, level in result.document.iterate_items():
                if isinstance(item, TableItem):
                    df = item.export_to_dataframe(result.document)
                    if not df.empty:
                        tables_segments.append(df)

            if not tables_segments:
                print("   ⚠️ Aucune table détectée.")
                continue

            ml_columns = [
                "Action",
                "Cours_ref",
                "Cours_Cloture",
                "Variation",
                "Volume",
                "Quantite"
            ]

            final_parts = []

            # ==============================
            # RECONSTRUCTION INTELLIGENTE
            # ==============================

            for part in tables_segments:

                rows_fixed = []

                for _, row in part.iterrows():

                    row_values = [x for x in row.tolist() if pd.notna(x)]

                    # On cherche au moins 6 éléments (1 texte + 5 chiffres)
                    if len(row_values) < 6:
                        continue

                    # Les 5 dernières valeurs sont numériques
                    numeric_values = row_values[-5:]

                    # Tout le reste = nom de l'action
                    action_name = " ".join(map(str, row_values[:-5])).strip()

                    # Ignore lignes parasites
                    if re.search(r"Action|Indice|Volume global|Nbre|Capitalisation",
                                 action_name, re.IGNORECASE):
                        continue

                    cleaned_numbers = [clean_numeric(val) for val in numeric_values]
                        # Filtre métier strict
                    cours_ref = cleaned_numbers[0]
                    cours_cloture = cleaned_numbers[1]
                    variation = cleaned_numbers[2]
                    volume = cleaned_numbers[3]
                    quantite = cleaned_numbers[4]

                    if (
                                cours_ref is None or
                                cours_cloture is None or
                                volume is None or
                                quantite is None
                            ):
                                continue

                    if cours_ref <= 0 or cours_cloture <= 0:
                                continue

                    if volume <= 0 or quantite <= 0:
                                continue

                    if variation is not None and abs(variation) > 0.5:
                                continue

                            # Ignore si le nom contient une date
                    if re.search(r"\d{2}/\d{2}/\d{4}", action_name):
                                continue

                            # Ignore si le nom est vide ou trop court
                    if len(action_name.strip()) < 3:
                                continue

                    rows_fixed.append([action_name] + cleaned_numbers)

                if rows_fixed:
                    df_part = pd.DataFrame(rows_fixed, columns=ml_columns)
                    final_parts.append(df_part)

            if not final_parts:
                print("   ⚠️ Aucune donnée action exploitable.")
                continue

            df_full = pd.concat(final_parts, ignore_index=True)

            # Suppression lignes invalides
            df_full = df_full.dropna(subset=["Cours_Cloture"])
            df_full = df_full[df_full["Cours_Cloture"] > 0]

            # ==============================
            # DATE
            # ==============================

            date_match = re.search(r"(\d{8})", pdf_path.name)

            if not date_match:
                print("   ⚠️ Date introuvable.")
                continue

            df_full["Date_Rapport"] = pd.to_datetime(
                date_match.group(1),
                format="%Y%m%d"
            )

            df_full = df_full.sort_values("Action").reset_index(drop=True)

            # ==============================
            # EXPORT
            # ==============================

            out_path = OUTPUT_DIR / f"{pdf_path.stem}_clean.csv"
            df_full.to_csv(out_path, index=False, encoding="utf-8-sig")

            print(f"   ✅ {len(df_full)} actions extraites correctement.")

        except Exception as e:
            print(f"   ❌ Erreur technique : {e}")


if __name__ == "__main__":
    main()