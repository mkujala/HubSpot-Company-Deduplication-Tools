# This script only tests that the HubSpot API is reachable and the token is valid.

import os
import requests
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")
if not HUBSPOT_TOKEN:
    raise RuntimeError("HUBSPOT_TOKEN is missing from .env")

url = "https://api.hubapi.com/crm/v3/objects/companies"
headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
params = {"limit": 1, "properties": "name,domain", "archived": "false"}

resp = requests.get(url, headers=headers, params=params, timeout=30)

if resp.status_code == 200:
    data = resp.json()
    print("✅ Connection OK")
    if data.get("results"):
        print("First company:", data["results"][0])
    else:
        print("No companies returned.")
else:
    print("❌ Error:", resp.status_code, resp.text)
