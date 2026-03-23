import os
import requests
import urllib3

# Désactivation des alertes SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

output_dir = "resumes_trimestriels_2023_v2"
os.makedirs(output_dir, exist_ok=True)

# 1. Cibles : On teste la fin théorique et les jours précédents (séances boursières)
trimestres_cibles = {
    "T1": ["20230331", "20230330", "20230329"],
    "T2": ["20230630", "20230629", "20230628"],
    "T3": ["20230930", "20230929", "20230928"],
    "T4": ["20231231", "20231230", "20231229"] # Votre exemple est ici
}

# 2. Dossiers à scanner
annees_dossiers = [2023, 2024]
mois_dossiers = [f"{m:02d}" for m in range(1, 13)]

print("🚀 Lancement du balayage haute précision pour 2023...")

for trimestre, dates in trimestres_cibles.items():
    found = False
    print(f"\n🔍 Recherche pour le {trimestre}...")
    
    for annee in annees_dossiers:
        if found: break
        for mois in mois_dossiers:
            if found: break
            dossier = f"{annee}-{mois}"
            
            for date_test in dates:
                url = f"https://media.casablanca-bourse.com/sites/default/files/{dossier}/resume_trimestriel_{date_test}.pdf"
                
                try:
                    response = requests.get(url, verify=False, timeout=2)
                    if response.status_code == 200:
                        path = os.path.join(output_dir, f"trimestre_{date_test}.pdf")
                        with open(path, "wb") as f:
                            f.write(response.content)
                        print(f"   ✅ TROUVÉ : {date_test} dans le dossier {dossier}")
                        found = True
                        break
                except Exception:
                    continue

    if not found:
        print(f"   ❌ {trimestre} introuvable après scan complet.")

print(f"\n✨ Terminé. Dossier : {output_dir}")