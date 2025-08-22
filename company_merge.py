# file: company_merge.py
import os
import re
import csv
import time
import argparse
import requests
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv

BASE = "https://api.hubapi.com"
BATCH_READ = "/crm/v3/objects/companies/batch/read"
MERGE = "/crm/v3/objects/companies/merge"
MAX_BATCH = 100
MAX_RETRIES = 5

# ---------- utils ----------
def load_token(cli_token: str | None) -> str:
    load_dotenv()
    token = cli_token or os.getenv("HUBSPOT_TOKEN")
    if not token:
        raise RuntimeError("HUBSPOT_TOKEN is missing (.env) and no - token parameter is given.")
    return token

def hs_iso_to_dt(iso_str: str | None):
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def request_with_retry(session, method, url, headers, **kwargs):
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.request(method, url, headers=headers, timeout=30, **kwargs)
        if resp.status_code not in (429, 500, 502, 503, 504):
            return resp
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                sleep_for = float(retry_after)
            except ValueError:
                sleep_for = attempt
        else:
            sleep_for = min(10.0, attempt * 1.5)
        time.sleep(sleep_for)
        last = resp
    return last or resp

def read_rows(path: str):
    """Returns a list of {id, domain, name} from CSV (semicolon-separated)."""
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        for r in reader:
            if not r:
                continue
            if r[0].strip().lower() == "id":
                continue
            rid = r[0].strip()
            domain = (r[1].strip().lower() if len(r) > 1 else "")
            name = (r[2].strip() if len(r) > 2 else "")
            rows.append({"id": rid, "domain": domain, "name": name})
    if not rows:
        raise RuntimeError("CSV is empty.")
    return rows

def batch_read_companies(session, headers, ids):
    """Returns {id: {'name','domain','created_dt','_raw_created'}} for all IDs."""
    out = {}
    for i in range(0, len(ids), MAX_BATCH):
        chunk = ids[i:i+MAX_BATCH]
        payload = {
            "properties": ["name", "domain", "hs_createdate", "createdate"],
            "idProperty": "hs_object_id",
            "inputs": [{"id": _id} for _id in chunk],
        }
        resp = request_with_retry(session, "POST", BASE + BATCH_READ, headers=headers, json=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"Batch read error {resp.status_code}: {resp.text}")
        data = resp.json()
        for r in data.get("results", []):
            rid = r.get("id")
            props = r.get("properties", {}) or {}
            raw_created = props.get("hs_createdate") or props.get("createdate")
            out[rid] = {
                "name": (props.get("name") or "").strip(),
                "domain": (props.get("domain") or "").strip().lower(),
                "created_dt": hs_iso_to_dt(raw_created),
                "_raw_created": raw_created,
            }
    missing = [i for i in ids if i not in out]
    if missing:
        raise RuntimeError(f"Following IDs were not found in HubSpot: {', '.join(missing)}")
    return out

def choose_primary(companies_dict):
    """companies_dict = {id: {created_dt: dt|None}} -> return primary_id."""
    if any(v.get("created_dt") for v in companies_dict.values()):
        return min(
            companies_dict.items(),
            key=lambda item: (item[1].get("created_dt") or datetime.max.replace(tzinfo=timezone.utc))
        )[0]
    # fallback: smallest numeric id
    return min(companies_dict.keys(), key=lambda s: int(s))

def resolve_canonical(session, headers, company_id: str) -> str:
    """Returns the canonical company ID (follows alias/forward references)."""
    url = f"{BASE}/crm/v3/objects/companies/{company_id}"
    resp = request_with_retry(session, "GET", url, headers=headers, params={"archived": "true"})
    if resp.status_code != 200:
        return company_id
    data = resp.json()
    return data.get("id") or company_id

def merge_pair(session, headers, primary_id, mergee_id, dry_run: bool):
    # ensure primary is canonical before merging
    primary_id = resolve_canonical(session, headers, primary_id)

    if dry_run:
        return {"status": "DRY_RUN", "primaryId": primary_id, "mergeeId": mergee_id}

    payload = {"primaryObjectId": primary_id, "objectIdToMerge": mergee_id}
    resp = request_with_retry(session, "POST", BASE + MERGE, headers=headers, json=payload)
    if resp.status_code in (200, 201, 204):
        return {"status": "MERGED", "primaryId": primary_id, "mergeeId": mergee_id}

    # 400 VALIDATION_ERROR: forward reference -> resolve canonical id and try again
    if resp.status_code == 400:
        m = re.search(r"forward reference to (\d+)", resp.text)
        if m:
            canonical = m.group(1)
            payload = {"primaryObjectId": canonical, "objectIdToMerge": mergee_id}
            resp2 = request_with_retry(session, "POST", BASE + MERGE, headers=headers, json=payload)
            if resp2.status_code in (200, 201, 204):
                return {"status": "MERGED", "primaryId": canonical, "mergeeId": mergee_id}
            raise RuntimeError(f"Merge retry error {resp2.status_code}: {resp2.text}")

    raise RuntimeError(f"Merge error {resp.status_code}: {resp.text}")

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(
        description="Merge duplicates given in CSV by domain (primary = oldest). Writes log to CSV."
    )
    ap.add_argument("csv_path", help="Semicolon-separated CSV with columns: id;domain;name")
    ap.add_argument("--token", help="HubSpot Private App token (alternative: .env: HUBSPOT_TOKEN)")
    ap.add_argument("--apply", action="store_true", help="Perform merge (without this it's a DRY-RUN)")
    args = ap.parse_args()

    token = load_token(args.token)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/json",
    }
    session = requests.Session()

    # 1) Read all rows and group by domain
    rows = read_rows(args.csv_path)
    groups = defaultdict(list)
    for r in rows:
        if not r["domain"]:
            continue
        groups[r["domain"]].append(r["id"])

    # Remove singles (only 1 ID per domain)
    groups = {d: list(dict.fromkeys(ids)) for d, ids in groups.items() if len(set(ids)) > 1}
    if not groups:
        print("CSV does not contain domain groups with more than one ID.")
        return

    # 2) Batch-read all unique IDs
    all_ids = sorted({i for ids in groups.values() for i in ids})
    info = batch_read_companies(session, headers, all_ids)

    # 3) Log file
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", f"merge_log_{ts}.csv")
    with open(log_path, "w", newline="", encoding="utf-8") as lf:
        writer = csv.writer(lf, delimiter=";")
        writer.writerow([
            "domain", "primary_id", "primary_name", "primary_created_raw",
            "mergee_id", "mergee_name", "mergee_created_raw", "status"
        ])

        total_mergees = 0
        for domain in sorted(groups.keys()):
            ids = groups[domain]
            sub = {i: info[i] for i in ids if i in info}
            if len(sub) < 2:
                continue

            # validate oldest and resolve canonical before starting
            primary = choose_primary(sub)
            primary = resolve_canonical(session, headers, primary)
            p = info.get(primary) or info[next(iter(sub.keys()))]
            mergees = [i for i in ids if i != primary]

            print(f"\n=== Domain: {domain} — {len(ids)} items (primary: {primary} / '{p['name']}')")
            print(f"    created: {p['created_dt']} (raw: {p['_raw_created']})")
            print(f"    mergee candidates: {len(mergees)}")

            for m in mergees:
                res = merge_pair(session, headers, primary, m, dry_run=(not args.apply))
                print(f"    {res['status']}: {res['mergeeId']} -> {res['primaryId']}")
                total_mergees += 1
                mi = info[m]
                writer.writerow([
                    domain, res["primaryId"], p["name"], p["_raw_created"],
                    m, mi["name"], mi["_raw_created"], res["status"]
                ])

    print(f"\nReady. {'Merged' if args.apply else 'Would merge'} total {total_mergees} items "
          f"{len(groups)} domain groups.")
    print(f"📄 Logs saved: {log_path}")

if __name__ == "__main__":
    main()
