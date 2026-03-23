import time
import imaplib
import email
import os
import re
from playwright.sync_api import sync_playwright

# --- CONFIGURATION ---
EMAIL_USER = "simolouw@gmail.com"
EMAIL_PASS = "qfyousyfwvsvzrcn" 
IMAP_SERVER = "imap.gmail.com"
DOWNLOAD_DIR = "./telechargements_bvc"

INSTRUMENTS = ["PROMOPHARM S.A."]
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def remplir_formulaire_et_valider(page, instrument):
    print(f"--- Traitement de l'instrument : {instrument} ---")
    page.goto("https://www.casablanca-bourse.com/fr/instruments")
    
    # 1. Sélection de l'instrument (Combobox/Autocomplete)
    input_instrument = page.get_by_role("combobox", name="Ns:Tout les emetteurs")
    input_instrument.click()
    input_instrument.fill(instrument)
    page.get_by_role("option", name=instrument, exact=True).click()
    
    # 2. Sélection de la période
    page.locator("#range-date").select_option(label="3 ans")
    
    # 3. Cliquer sur Appliquer
    page.get_by_role("button", name="Appliquer").first.click()
    
    # Attente du chargement
    page.locator("a:has-text('Télécharger')").wait_for(state="visible")
    page.get_by_role("link", name="Télécharger").click()
    
    # 4. Remplissage du formulaire
    page.locator("input[placeholder='Entrez le numéro de téléphone']").fill("658145425")
    page.locator("#e_mail").fill(EMAIL_USER)
    
    # 7. Cocher la case
    page.get_by_text("J'ai lu et j'accepte les mentions légales").click()
    page.locator("#mention_legale").check(force=True)
    
    # 5. Envoyer le formulaire
    page.get_by_role("button", name="Envoyer").click()
    print(f"Formulaire envoyé pour {instrument}. En attente du mail...")

def extraire_lien_depuis_mail(instrument):
    """Se connecte au mail et extrait l'URL du bouton Télécharger via Regex"""
    time.sleep(100) 
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")
        
        # Recherche du mail non lu
        status, data = mail.search(None, f'(UNSEEN FROM "noreply@casablanca-bourse.com" SUBJECT "Bourse de Casablanca - Historique des instruments (Actions)")')
        
        ids = data[0].split()
        if not ids:
            mail.close()
            mail.logout()
            return None

        num = ids[-1]
        status, res = mail.fetch(num, "(RFC822)")
        msg = email.message_from_bytes(res[0][1])
            
            # Extraction du contenu HTML pour trouver le lien
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html_body = part.get_payload(decode=True).decode()
                    # Regex pour capturer l'URL de téléchargement sur le domaine BVC
                links = re.findall(r'href=["\'](https://(?:www\.)?media\.casablanca-bourse\.com/[^"\']+)["\']', html_body)
                if links:
                    mail.store(num, '+FLAGS', '\\Seen') # Marquer comme lu
                    mail.close()
                    mail.logout()
                    return links[0]
    except Exception as e:
        print(f"Erreur IMAP : {e}")
    return None

def orchestrateur():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        for instrument in INSTRUMENTS:
                # Étape Web 1 : Formulaire
                remplir_formulaire_et_valider(page, instrument)
                
                # Étape Email : Récupérer le lien
                lien_telechargement = None
                tentatives = 0
                while not lien_telechargement and tentatives < 3:
                    lien_telechargement = extraire_lien_depuis_mail(instrument)
                    if not lien_telechargement:
                        print(f"Lien non trouvé pour {instrument}, attente...")
                        time.sleep(300)
                        tentatives += 1
                
                # Étape Web 2 : Cliquer sur le lien reçu par mail
                if lien_telechargement:
                    print(f"Lien de téléchargement trouvé : {lien_telechargement}")
                    try:
                        with page.expect_download() as download_info:
                            page.evaluate(f"window.location.href = '{lien_telechargement}'")
                        download = download_info.value
                        path=os.path.join(DOWNLOAD_DIR, f"{instrument}_data.xlsx")
                        download.save_as(path)
                        print(f"Fichier téléchargé pour {instrument} : {path}")
                    except Exception as e:
                        print(f"Erreur lors du téléchargement pour {instrument} : {e}")
        browser.close()

if __name__ == "__main__":
    orchestrateur()