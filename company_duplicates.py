# file: company_duplicates.py
import os
import re
import csv
import time
import argparse
import requests
from collections import defaultdict, Counter
from datetime import datetime
from dotenv import load_dotenv

# --- HubSpot API endpoints ---
BASE = "https://api.hubapi.com"
COMPANY_LIST = "/crm/v3/objects/companies"
ASSOC_BATCH_READ = "/crm/v3/associations/companies/contacts/batch/read"
CONTACT_BATCH_READ = "/crm/v3/objects/contacts/batch/read"

PAGE_LIMIT = 100
BATCH_SIZE = 100
MAX_RETRIES = 5

# Common freemail domains to ignore when deriving org domains from contacts
FREEMAIL = {
    "gmail.com", "outlook.com", "hotmail.com", "live.com", "yahoo.com", "icloud.com",
    "me.com", "msn.com", "aol.com", "proton.me", "protonmail.com", "mail.com", "gmx.com"
}

# ---------- helpers ----------
def load_token() -> str:
    load_dotenv()
    t = os.getenv("HUBSPOT_TOKEN")
    if not t:
        raise RuntimeError("HUBSPOT_TOKEN missing from .env")
    return t

def session_with_headers(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/json",
    })
    return s

def request_with_retry(s: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    """
    HTTP request with retries for rate limiting / transient server errors.
    Respects Retry-After when present.
    """
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        r = s.request(method, url, timeout=30, **kwargs)
        if r.status_code not in (429, 500, 502, 503, 504):
            return r
        ra = r.headers.get("Retry-After")
        try:
            sleep_for = float(ra) if ra else min(10.0, attempt * 1.5)
        except ValueError:
            sleep_for = min(10.0, attempt * 1.5)
        time.sleep(sleep_for)
        last = r
    return last or r

def norm_domain(v: str | None) -> str:
    if not v:
        return ""
    d = v.strip().lower().rstrip(".")
    if d.startswith("www."):
        d = d[4:]
    return d

def norm_name(v: str | None) -> str:
    if not v:
        return ""
    n = v.casefold().strip()
    n = re.sub(r"[\s\-_]+", " ", n)
    n = re.sub(r"[^\w\s]", "", n, flags=re.UNICODE)
    # conservatively remove some common suffixes
    n = re.sub(r"\b(oy|ab|ltd|inc|oyj)\b\.?", "", n).strip()
    return n

def email_to_domain(email: str | None) -> str:
    if not email or "@" not in email:
        return ""
    return norm_domain(email.split("@", 1)[1])

# ---------- data fetching ----------
def fetch_all_companies(s: requests.Session) -> list[dict]:
    """
    Returns a list of dicts: {id, name, domain}
    """
    out = []
    params = {"limit": PAGE_LIMIT, "archived": "false", "properties": "name,domain"}
    after = None
    while True:
        if after:
            params["after"] = after
        r = request_with_retry(s, "GET", BASE + COMPANY_LIST, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Company list error {r.status_code}: {r.text}")
        data = r.json()
        for item in data.get("results", []):
            p = item.get("properties", {}) or {}
            out.append({
                "id": item["id"],
                "name": (p.get("name") or "").strip(),
                "domain": norm_domain(p.get("domain")),
            })
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return out

def batch_read_associations_company_contacts(s: requests.Session, company_ids: list[str]) -> dict[str, list[str]]:
    """
    Returns {company_id: [contact_ids]} using associations batch read.
    Handles HTTP 207 (Multi-Status) by parsing successes from 'results'.
    """
    out: dict[str, list[str]] = defaultdict(list)
    for i in range(0, len(company_ids), BATCH_SIZE):
        chunk = company_ids[i:i + BATCH_SIZE]
        payload = {"inputs": [{"id": cid} for cid in chunk]}
        r = request_with_retry(s, "POST", BASE + ASSOC_BATCH_READ, json=payload)

        if r.status_code not in (200, 207):
            raise RuntimeError(f"Assoc batch read error {r.status_code}: {r.text}")

        data = r.json() if r.text else {}
        for row in (data.get("results") or []):
            from_id = row.get("fromId")
            tos = [t.get("toObjectId") for t in row.get("to", []) if t.get("toObjectId")]
            if from_id and tos:
                out[from_id].extend(tos)
        # Optionally inspect per-item errors in data.get("errors")

    return out

def batch_read_contacts_emails(s: requests.Session, contact_ids: list[str]) -> dict[str, str]:
    """
    Returns {contact_id: email} using contacts batch read.
    Handles HTTP 207 similarly by reading 'results'.
    """
    out: dict[str, str] = {}
    for i in range(0, len(contact_ids), BATCH_SIZE):
        chunk = contact_ids[i:i + BATCH_SIZE]
        payload = {
            "properties": ["email"],
            "idProperty": "hs_object_id",
            "inputs": [{"id": cid} for cid in chunk],
        }
        r = request_with_retry(s, "POST", BASE + CONTACT_BATCH_READ, json=payload)
        if r.status_code not in (200, 207):
            raise RuntimeError(f"Contact batch read error {r.status_code}: {r.text}")
        data = r.json() if r.text else {}
        for row in (data.get("results") or []):
            rid = row.get("id")
            props = row.get("properties", {}) or {}
            out[rid] = (props.get("email") or "").strip()
    return out

def derive_contact_domain_for_companies(
    s: requests.Session,
    company_ids: list[str],
    only_ids_without_domain: set[str] | None = None
) -> dict[str, str]:
    """
    For each company, derive the most common non-freemail email domain from associated contacts.
    If only_ids_without_domain is provided, compute only for that subset.
    Returns {company_id: contact_domain or ""}.
    """
    target_ids = list(only_ids_without_domain) if only_ids_without_domain else company_ids
    if not target_ids:
        return {cid: "" for cid in company_ids}

    assoc = batch_read_associations_company_contacts(s, target_ids)
    all_contact_ids = sorted({cid for ids in assoc.values() for cid in ids})
    if not all_contact_ids:
        return {cid: "" for cid in company_ids}

    emails = batch_read_contacts_emails(s, all_contact_ids)

    result: dict[str, str] = {cid: "" for cid in company_ids}
    for comp_id, cids in assoc.items():
        domains = [
            d for d in (email_to_domain(emails.get(c)) for c in cids)
            if d and d not in FREEMAIL
        ]
        if domains:
            common, _ = Counter(domains).most_common(1)[0]
            result[comp_id] = common
    return result

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(
        description="Find HubSpot company duplicates by domain, normalized name, and contact-derived email domains."
    )
    # All strategies ON by default; can be disabled with flags
    ap.add_argument("--no-by-domain", action="store_true", help="Disable grouping by company domain.")
    ap.add_argument("--no-by-name", action="store_true", help="Disable grouping by normalized company name.")
    ap.add_argument("--no-by-contact-domain", action="store_true", help="Disable grouping by contact-derived domain.")
    args = ap.parse_args()

    use_by_domain = not args.no_by_domain
    use_by_name = not args.no_by_name
    use_by_contact_domain = not args.no_by_contact_domain

    token = load_token()
    s = session_with_headers(token)

    print("Fetching companies ...")
    companies = fetch_all_companies(s)
    total = len(companies)
    no_domain_count = sum(1 for c in companies if not c["domain"])
    print(f"Fetched {total} companies (without domain: {no_domain_count}).")
    print(f"Rules -> by_domain={use_by_domain}, by_name={use_by_name}, by_contact_domain={use_by_contact_domain}")

    # Grouping by domain and normalized name
    by_domain: dict[str, list[dict]] = defaultdict(list)
    by_name: dict[str, list[dict]] = defaultdict(list)
    all_company_ids: list[str] = []

    for c in companies:
        all_company_ids.append(c["id"])
        if use_by_domain and c["domain"]:
            by_domain[c["domain"]].append(c)
        if use_by_name:
            nn = norm_name(c["name"])
            if nn:
                by_name[nn].append(c)

    # Contact-derived domains for companies missing a domain only
    by_contact_domain: dict[str, list[dict]] = defaultdict(list)
    contact_domains: dict[str, str] = {}
    derived_count = 0
    if use_by_contact_domain:
        print("Deriving contact-based domains (associations + contact emails) ...")
        ids_without_domain = {c["id"] for c in companies if not c["domain"]}
        contact_domains = derive_contact_domain_for_companies(
            s, all_company_ids, only_ids_without_domain=ids_without_domain
        )
        for c in companies:
            cd = contact_domains.get(c["id"], "")
            if cd:
                derived_count += 1
                by_contact_domain[cd].append(c)
        print(f"Derived contact-domain for {derived_count} companies (non-freemail).")

    # Collect duplicate rows
    dup_rows = []

    def add_grouping(group_dict, label: str):
        groups = 0
        rows_before = len(dup_rows)
        for key, items in group_dict.items():
            uniq = {i["id"]: i for i in items}
            if len(uniq) > 1:
                groups += 1
                for it in uniq.values():
                    dup_rows.append([it["id"], it["domain"], it["name"], label, key])
        return groups, len(dup_rows) - rows_before

    g_dom = g_name = g_cdom = r_dom = r_name = r_cdom = 0
    if use_by_domain:
        g_dom, r_dom = add_grouping(by_domain, "company_domain")
    if use_by_name:
        g_name, r_name = add_grouping(by_name, "company_name")
    if use_by_contact_domain:
        g_cdom, r_cdom = add_grouping(by_contact_domain, "contact_domain")

    if not dup_rows:
        print("No duplicates found with the selected criteria.")
        return

    # Write CSV
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    os.makedirs("data", exist_ok=True)
    out_path = os.path.join("data", f"duplicates_{ts}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["id", "domain", "name", "match_type", "match_key"])
        for row in sorted(dup_rows, key=lambda r: (r[3], r[4], r[2], r[0])):
            w.writerow(row)

    print(f"\n✅ Saved {len(dup_rows)} rows to {out_path}")
    print(f"Groups found -> by_domain: {g_dom} (rows {r_dom}), by_name: {g_name} (rows {r_name}), contact_domain: {g_cdom} (rows {r_cdom})")
    if use_by_contact_domain:
        print("Note: contact_domain derived from associated contacts' emails (freemail domains ignored).")
        print("Required token scopes: crm.objects.companies.read, crm.associations.read, crm.objects.contacts.read")

if __name__ == "__main__":
    main()
