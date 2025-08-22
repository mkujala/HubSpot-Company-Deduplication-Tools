# file: company_duplicates_fast_to_csv.py
import os
import time
import requests
from collections import defaultdict
from dotenv import load_dotenv
from datetime import datetime
import csv

# Optional dependency for robust domain normalization (punycode/IDNA)
try:
    import idna  # pip install idna
except Exception:  # pragma: no cover
    idna = None

load_dotenv()
TOKEN = os.getenv("HUBSPOT_TOKEN")
if not TOKEN:
    raise RuntimeError("HUBSPOT_TOKEN puuttuu .env-tiedostosta")

BASE = "https://api.hubapi.com"
LIST = "/crm/v3/objects/companies"
BATCH_READ = "/crm/v3/objects/companies/batch/read"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept-Encoding": "gzip, deflate",
    "Content-Type": "application/json",
}

PAGE_LIMIT = 100
MAX_BATCH = 100
MAX_RETRIES = 5

session = requests.Session()

def norm_domain(value: str) -> str:
    """Normalize domain: trim, lowercase, strip trailing dot, drop 'www.', IDNA encode."""
    d = (value or "").strip().lower().rstrip(".")
    if d.startswith("www."):
        d = d[4:]
    if idna and d:
        try:
            d = idna.encode(d).decode("ascii")
        except Exception:
            # If it fails, keep best-effort normalization
            pass
    return d

def _request(method, url, **kwargs):
    """HTTP request with retries on 429 / 5xx and Retry-After support."""
    last_resp = None
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.request(method, url, timeout=30, headers=HEADERS, **kwargs)
        status = resp.status_code
        if status not in (429, 500, 502, 503, 504):
            return resp
        # Respect Retry-After if present (can be integer seconds)
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                sleep_for = float(retry_after)
            except ValueError:
                sleep_for = attempt  # fallback
        else:
            # Exponential-ish backoff with cap
            sleep_for = min(10.0, attempt * 1.5)
        time.sleep(sleep_for)
        last_resp = resp
    # Return last response (likely an error) after exhausting retries
    return last_resp if last_resp is not None else resp

def fetch_ids_and_domains():
    """Pass 1: fetch only id + normalized domain for all companies (non-empty)."""
    params = {"limit": PAGE_LIMIT, "archived": "false", "properties": "domain"}
    after = None
    while True:
        if after:
            params["after"] = after
        resp = _request("GET", BASE + LIST, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"List error {resp.status_code}: {resp.text}")
        data = resp.json()
        for item in data.get("results", []):
            props = item.get("properties", {}) or {}
            domain = norm_domain(props.get("domain"))
            if domain:
                yield {"id": item["id"], "domain": domain}
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

def batch_read_names(ids):
    """Pass 2: fetch names (and domain as sanity) for the given ids via batch/read."""
    out = {}
    for i in range(0, len(ids), MAX_BATCH):
        chunk = ids[i:i + MAX_BATCH]
        payload = {
            "properties": ["name", "domain"],
            "idProperty": "hs_object_id",
            "inputs": [{"id": _id} for _id in chunk],
        }
        resp = _request("POST", BASE + BATCH_READ, json=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"Batch read error {resp.status_code}: {resp.text}")
        data = resp.json()
        for r in data.get("results", []):
            rid = r.get("id")
            props = r.get("properties", {}) or {}
            out[rid] = {
                "name": (props.get("name") or "").strip(),
                "domain": norm_domain(props.get("domain")),
            }
    return out

def main():
    # Group ids by normalized domain
    groups = defaultdict(list)
    for row in fetch_ids_and_domains():
        groups[row["domain"]].append(row["id"])

    # Collect ids that belong to duplicate domains
    dup_ids = []
    for domain, idlist in groups.items():
        if len(idlist) > 1:
            dup_ids.extend(idlist)

    if not dup_ids:
        print("No domain duplicates found.")
        return

    details = batch_read_names(dup_ids)

    # Prepare rows id | domain | name
    rows = []
    for _id in dup_ids:
        info = details.get(_id, {})
        rows.append((_id, info.get("domain", ""), info.get("name", "")))

    # Output filename with timestamp (sortable)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = "data"
    os.makedirs(out_dir, exist_ok=True)
    filename = os.path.join(out_dir, f"duplicates_{ts}.csv")

    # Write as semicolon-separated CSV
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["id", "domain", "name"])
        for row in sorted(rows, key=lambda x: (x[1], x[2], x[0])):
            writer.writerow(row)

    print(f"✅ Tallennettu {len(rows)} riviä tiedostoon {filename}")

if __name__ == "__main__":
    main()
