# This script is only for testing that Hubspot API is reachable and The token is valid.

import os
import requests
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")

if not HUBSPOT_TOKEN:
    raise RuntimeError("HUBSPOT_TOKEN puuttuu .env-tiedostosta")

url = "https://api.hubapi.com/crm/v3/objects/companies"
headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
params = {"limit": 1, "properties": "name,domain"}

resp = requests.get(url, headers=headers, params=params)

if resp.status_code == 200:
    data = resp.json()
    print("✅ Yhteys toimii!")
    if data.get("results"):
        print("Ensimmäinen yritys:", data["results"][0])
    else:
        print("Ei löytynyt yrityksiä.")
else:
    print("❌ Virhe:", resp.status_code, resp.text)
