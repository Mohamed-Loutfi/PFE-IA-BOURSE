import os
import requests
from datetime import date, timedelta
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Dossier de destination
output_dir = "resumes_seance_2022"
os.makedirs(output_dir, exist_ok=True)

def get_trading_days(year):
    # Génère les dates pour les jours de semaine (Lundi-Vendredi)
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    delta = end_date - start_date
    
    return [
        (start_date + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(delta.days + 1)
        if (start_date + timedelta(days=i)).weekday() < 5
    ]

# Boucle de téléchargement
trading_days = get_trading_days(2022)

for d in trading_days:
    url = f"https://media.casablanca-bourse.com/sites/default/files/es-auto-upload/fr/resume_seance_{d}.pdf"
    response = requests.get(url,verify=False)
    
    if response.status_code == 200:
        with open(f"{output_dir}/resume_{d}.pdf", "wb") as f:
            f.write(response.content)
        print(f"✅ Téléchargé : {d}")
    else:
        # Ignore les jours fériés ou erreurs sans stopper le script
        print(f"❌ Non trouvé (probablement férié) : {d}")