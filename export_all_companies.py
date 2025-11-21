#!/usr/bin/env python
import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional

import requests
from dotenv import load_dotenv

# Load .env
load_dotenv()

HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")
HUBSPOT_BASE = "https://api.hubapi.com"


def get_session_and_headers() -> Tuple[requests.Session, Dict[str, str]]:
    if not HUBSPOT_TOKEN:
        print("ERROR: HUBSPOT_TOKEN is not set in environment (.env).")
        sys.exit(1)

    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    return session, headers


def fetch_all_companies(
    session: requests.Session,
    headers: Dict[str, str],
    properties: List[str],
    limit: int,
    max_count: Optional[int],
) -> List[Dict[str, Any]]:
    """
    Fetch companies from HubSpot up to max_count if provided.
    """
    url = f"{HUBSPOT_BASE}/crm/v3/objects/companies"
    params: Dict[str, Any] = {
        "limit": limit,
        "archived": "false",
        "properties": ",".join(properties),
    }

    results: List[Dict[str, Any]] = []
    after: Optional[str] = None

    while True:
        if after is not None:
            params["after"] = after

        resp = session.get(url, headers=headers, params=params)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "3"))
            print(f"Rate limited (429). Sleeping {retry_after} seconds...")
            time.sleep(retry_after)
            continue

        if resp.status_code != 200:
            print(f"ERROR: fetch_all_companies HTTP {resp.status_code}: {resp.text}")
            sys.exit(1)

        data = resp.json()
        batch = data.get("results", [])
        results.extend(batch)

        if max_count is not None and len(results) >= max_count:
            results = results[:max_count]
            print(f"Max count {max_count} reached, stopping early.")
            break

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

        print(f"Fetched {len(results)} companies so far...")

    print(f"Fetched total {len(results)} companies.")
    return results


def fetch_company(
    session: requests.Session,
    headers: Dict[str, str],
    company_id: str,
    props: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    if props is None:
        props = ["hs_canonical_object_id", "createdate", "name", "domain"]

    url = f"{HUBSPOT_BASE}/crm/v3/objects/companies/{company_id}"
    params = {"properties": ",".join(props), "archived": "false"}
    resp = session.get(url, headers=headers, params=params)

    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        print(f"WARNING: fetch_company({company_id}) HTTP {resp.status_code}: {resp.text}")
        return None

    return resp.json()


def resolve_canonical_id(
    session: requests.Session,
    headers: Dict[str, str],
    cache: Dict[str, str],
    company_id: str,
    initial_properties: Optional[Dict[str, Any]] = None,
    max_depth: int = 10,
) -> str:
    """
    Resolve the final canonical company ID for the given company_id
    by following hs_canonical_object_id until it is empty or stable.
    """
    if company_id in cache:
        return cache[company_id]

    current_id = company_id
    depth = 0
    props = initial_properties.copy() if initial_properties else None

    while depth < max_depth:
        depth += 1

        if props is None:
            obj = fetch_company(session, headers, current_id, props=["hs_canonical_object_id"])
            if obj is None:
                cache[company_id] = current_id
                return current_id
            props = obj.get("properties", {}) or {}

        canonical_prop = (props.get("hs_canonical_object_id") or "").strip()

        if not canonical_prop:
            cache[company_id] = current_id
            return current_id

        if canonical_prop == current_id:
            cache[company_id] = current_id
            return current_id

        current_id = canonical_prop
        props = None

    cache[company_id] = current_id
    return current_id


def parse_createdate(props: Dict[str, Any]) -> str:
    raw = props.get("createdate")
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""


def build_output_rows(
    session: requests.Session,
    headers: Dict[str, str],
    companies: List[Dict[str, Any]],
    include_merged_history: bool,
) -> List[Dict[str, Any]]:
    """
    Build rows for CSV output.

    By default only canonical endpoints are included.
    If include_merged_history is True, all companies are exported.
    """
    rows: List[Dict[str, Any]] = []
    canonical_cache: Dict[str, str] = {}

    for obj in companies:
        cid = obj.get("id")
        props = obj.get("properties", {}) or {}

        raw_name = props.get("name") or ""
        raw_domain = props.get("domain") or ""
        raw_canonical = (props.get("hs_canonical_object_id") or "").strip()
        created_iso = parse_createdate(props)

        resolved_canonical = resolve_canonical_id(
            session, headers, canonical_cache, cid, initial_properties=props
        )
        is_canonical = "1" if resolved_canonical == cid else "0"

        if not include_merged_history and is_canonical != "1":
            continue

        row = {
            "id": cid,
            "name": raw_name,
            "domain": raw_domain,
            "createdate": created_iso,
            "hs_canonical_object_id": raw_canonical,
            "resolved_canonical_id": resolved_canonical,
            "is_canonical": is_canonical,
        }
        rows.append(row)

    return rows


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    fieldnames = [
        "id",
        "name",
        "domain",
        "createdate",
        "hs_canonical_object_id",
        "resolved_canonical_id",
        "is_canonical",
    ]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Wrote {len(rows)} rows to {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export HubSpot companies to CSV with canonical information. "
            "By default exports only canonical endpoints. Use --include-merged-history "
            "to include also merged (non-canonical) companies."
        )
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output CSV path. Default: data/all_companies_<timestamp>.csv",
    )
    parser.add_argument(
        "--include-merged-history",
        action="store_true",
        help="Include all companies (also non-canonical / merged history).",
    )
    parser.add_argument(
        "--max-count",
        type=int,
        help="Max number of companies to export (for safe testing).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="API page size (max 100). Default: 100.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session, headers = get_session_and_headers()

    if args.output:
        out_path = args.output
    else:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_path = os.path.join("data", f"all_companies_{ts}.csv")

    properties = [
        "name",
        "domain",
        "createdate",
        "hs_canonical_object_id",
    ]

    print("Fetching companies from HubSpot...")
    companies = fetch_all_companies(
        session=session,
        headers=headers,
        properties=properties,
        limit=args.limit,
        max_count=args.max_count,
    )

    print("Building output rows with canonical information...")
    rows = build_output_rows(
        session=session,
        headers=headers,
        companies=companies,
        include_merged_history=args.include_merged_history,
    )

    write_csv(out_path, rows)


if __name__ == "__main__":
    main()
