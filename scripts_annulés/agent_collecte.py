import os
import sys
import asyncio
from langchain_google_genai import ChatGoogleGenerativeAI
from browser_use import Agent

# Correction Windows spécifique aux scripts .py
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

os.environ["GOOGLE_API_KEY"] = ""

# Wrapper de compatibilité
class GeminiWrapper:
    def __init__(self, llm):
        self.llm = llm
        self.provider = "google"
        self.model_name = llm.model 
    def __getattr__(self, name):
        return getattr(self.llm, name)

async def run_agent():
    llm_original = ChatGoogleGenerativeAI(model="gemini-2.0-flash")
    llm = GeminiWrapper(llm_original)
    
    agent = Agent(
        task="""
        1. Attends que la page https://www.casablanca-bourse.com/fr/editions-statistiques soit totalement chargée.
        2. Trouve le menu déroulant qui contient 'Toutes les éditions statistiques' et clique dessus.
        3. Clique sur l'option 'Résumé de séance'.
        4. Tape '01/01/2024' dans le champ de date de début et '31/01/2024' dans le champ de date de fin.
        5. Clique sur le bouton 'Appliquer' ou 'Rechercher'.
        6. Liste les liens PDF présents et télécharge-les.
        """,
        llm=llm,
        calculate_cost=False,
        generate_gif=True
    )
    history = await agent.run()
    print("\n--- DIAGNOSTIC FINAL ---")
    if history.errors():
        print(f"Erreurs détectées : {history.errors()}")
    else:
        print("L'agent pense avoir réussi, vérifiez le dossier /resumes_seance.")
if __name__ == "__main__":
    asyncio.run(run_agent())

