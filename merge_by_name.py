#!/usr/bin/env python

# NOTE:
# This module is intended to be used as a library by other scripts
# (for example company_merge.py, manual_review_merge.py and merge_fuzzy_ids.py).
# In the current workflow it is not meant to be run directly as a standalone CLI.
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Set, Optional

import requests
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Read HubSpot token from environment
HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")
HUBSPOT_BASE = "https://api.hubapi.com"


def get_session_and_headers() -> Tuple[requests.Session, Dict[str, str]]:
    """
    Initialize a requests session and build default headers for HubSpot API.
    """
    if not HUBSPOT_TOKEN:
        print("ERROR: HUBSPOT_TOKEN is not set in environment (.env).")
        sys.exit(1)

    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    return session, headers


def hubspot_company_search(
    session: requests.Session,
    headers: Dict[str, str],
    name: str,
    operator: str,
) -> List[Dict[str, Any]]:
    """
    Low level helper to search companies by name with a given operator.

    operator:
      - "EQ" for exact match
      - "CONTAINS_TOKEN" for token based fuzzy search
    """
    url = f"{HUBSPOT_BASE}/crm/v3/objects/companies/search"
    body: Dict[str, Any] = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "name", "operator": operator, "value": name}
                ]
            }
        ],
        "properties": ["name", "domain", "createdate", "hs_canonical_object_id"],
        "limit": 100,
    }

    results: List[Dict[str, Any]] = []
    after: Optional[str] = None

    while True:
        if after is not None:
            body["after"] = after
        resp = session.post(url, headers=headers, json=body)
        if resp.status_code != 200:
            print(
                f"ERROR: hubspot_company_search({operator}) HTTP {resp.status_code}: {resp.text}"
            )
            sys.exit(1)
        data = resp.json()
        results.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return results


def search_companies_eq(
    session: requests.Session,
    headers: Dict[str, str],
    name: str,
) -> List[Dict[str, Any]]:
    """
    Exact match search (EQ) for company name.
    """
    return hubspot_company_search(session, headers, name, operator="EQ")


def search_companies_fuzzy(
    session: requests.Session,
    headers: Dict[str, str],
    name: str,
) -> List[Dict[str, Any]]:
    """
    Fuzzy search using CONTAINS_TOKEN for company name.
    Used only for interactive confirmation, never auto merged.
    """
    return hubspot_company_search(session, headers, name, operator="CONTAINS_TOKEN")


def parse_createdate_from_properties(props: Dict[str, Any]) -> datetime:
    """
    Parse HubSpot createdate property to timezone aware datetime.

    If createdate is missing or invalid, returns datetime.max in UTC
    so that it sorts as "latest".
    """
    raw = props.get("createdate")
    if not raw:
        return datetime.max.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.max.replace(tzinfo=timezone.utc)


def parse_createdate(obj: Dict[str, Any]) -> datetime:
    """
    Convenience wrapper for objects that contain a "properties" payload.
    """
    props = obj.get("properties", {}) or {}
    return parse_createdate_from_properties(props)


def fetch_company(
    session: requests.Session,
    headers: Dict[str, str],
    company_id: str,
    props: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch a single company by ID.

    Returns:
      - full JSON object if found
      - None if not found (404) or on other non 200 errors
    """
    if props is None:
        props = ["hs_canonical_object_id", "createdate", "name", "domain"]

    url = f"{HUBSPOT_BASE}/crm/v3/objects/companies/{company_id}"
    params = {
        "properties": ",".join(props),
        "archived": "false",
    }
    resp = session.get(url, headers=headers, params=params)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        print(f"ERROR: fetch_company({company_id}) HTTP {resp.status_code}: {resp.text}")
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
    Resolve the final canonical company ID for a given company ID.

    Follows hs_canonical_object_id chain until it is empty or stable.
    Uses a small depth limit to avoid accidental infinite loops.
    """
    if company_id in cache:
        return cache[company_id]

    current_id = company_id
    depth = 0
    props = initial_properties.copy() if initial_properties else None

    while depth < max_depth:
        depth += 1
        if props is None:
            obj = fetch_company(
                session, headers, current_id, props=["hs_canonical_object_id", "createdate"]
            )
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


def merge_pair(
    session: requests.Session,
    headers: Dict[str, str],
    primary_id: str,
    secondary_id: str,
) -> Tuple[bool, str]:
    """
    Perform one HubSpot merge API call.

    Returns:
      (True, "MERGED") on success
      (False, "HTTP ...") with raw error on failure
    """
    url = f"{HUBSPOT_BASE}/crm/v3/objects/companies/merge"
    payload = {"primaryObjectId": primary_id, "objectIdToMerge": secondary_id}
    resp = session.post(url, headers=headers, json=payload)
    if resp.status_code == 200:
        return True, "MERGED"
    return False, f"HTTP {resp.status_code}: {resp.text}"


def merge_companies_for_name(
    session: requests.Session,
    headers: Dict[str, str],
    name: str,
    dry_run: bool,
    sleep_seconds: float = 0.3,
) -> Tuple[int, int, bool, bool, bool, List[str]]:
    """
    Merge all companies with the given name into a single canonical.

    Returns:
      success_count
      failure_count
      had_any_companies   -> whether any company was found at all
      fuzzy_candidates_found
      fuzzy_merge_performed
      merged_pairs        -> list of "from_name -> to_name" strings
    """
    success_count = 0
    failure_count = 0
    fuzzy_candidates_found = False
    fuzzy_merge_performed = False
    merged_pairs: List[str] = []

    print(f"\n=== Name: {name} ===")

    # 1. Exact match search
    companies = search_companies_eq(session, headers, name)

    # 2. If no exact match, try fuzzy search and ask for confirmation
    if len(companies) == 0:
        fuzzy_companies = search_companies_fuzzy(session, headers, name)
        if len(fuzzy_companies) == 0:
            print("  No companies found (exact or fuzzy).")
            return success_count, failure_count, False, False, False, merged_pairs

        fuzzy_candidates_found = True
        print("  No exact EQ matches, but fuzzy search (CONTAINS_TOKEN) returned:")
        for c in fuzzy_companies:
            cid = c["id"]
            props = c.get("properties", {}) or {}
            cname = props.get("name") or ""
            created = parse_createdate(c)
            print(f"    - ID {cid}, name '{cname}', created {created.isoformat()}")

        if dry_run:
            print(
                "  DRY RUN: would ask for confirmation to fuzzy merge, skipping for this name."
            )
            return success_count, failure_count, True, True, False, merged_pairs

        answer = input(
            f"  Do you want to fuzzy-merge name '{name}' into the companies listed above? [y/N]: "
        ).strip().lower()

        if answer not in ("y", "yes"):
            print("  Skipping fuzzy merge for this name.")
            return success_count, failure_count, True, True, False, merged_pairs

        print("  Proceeding with fuzzy merge for this name.")
        companies = fuzzy_companies
        fuzzy_merge_performed = True

    # If still only one company, nothing to merge
    if len(companies) == 1:
        c = companies[0]
        print(f"  Only one company found (ID {c['id']}). Nothing to merge.")
        return success_count, failure_count, True, fuzzy_candidates_found, fuzzy_merge_performed, merged_pairs

    print(f"  Found {len(companies)} companies with this name.")

    # Build a mapping for easy name lookup
    company_objs: Dict[str, Dict[str, Any]] = {c["id"]: c for c in companies}

    def company_name(company_id: str) -> str:
        obj = company_objs.get(company_id)
        if not obj:
            return company_id
        props = obj.get("properties", {}) or {}
        n = props.get("name") or ""
        return n if n else company_id

    # Resolve canonical IDs
    canonical_cache: Dict[str, str] = {}
    all_ids: List[str] = []
    canonical_ids: Set[str] = set()

    for c in companies:
        cid = c["id"]
        props = c.get("properties", {}) or {}
        all_ids.append(cid)
        canonical_id = resolve_canonical_id(
            session,
            headers,
            canonical_cache,
            cid,
            initial_properties=props,
        )
        canonical_ids.add(canonical_id)
        print(f"    Company {cid} ('{company_name(cid)}') -> canonical {canonical_id}")

    # Determine final primary canonical
    if len(canonical_ids) == 1:
        final_primary_id = next(iter(canonical_ids))
        print(f"  Single canonical for this name: {final_primary_id}")
    else:
        print(f"  Multiple canonical IDs found for this name: {', '.join(canonical_ids)}")
        canonical_list: List[Tuple[str, datetime]] = []
        for canon_id in canonical_ids:
            obj = fetch_company(
                session, headers, canon_id, props=["createdate", "name", "domain"]
            )
            if obj is None:
                created = datetime.max.replace(tzinfo=timezone.utc)
            else:
                created = parse_createdate(obj)
                company_objs.setdefault(canon_id, obj)
            canonical_list.append((canon_id, created))
            print(f"    Canonical candidate {canon_id}, created {created.isoformat()}")

        canonical_list.sort(key=lambda t: t[1])
        final_primary_id = canonical_list[0][0]
        print(f"  Selected final primary canonical ID {final_primary_id} (oldest createdate).")

    print("  All candidate companies for this name:")
    for cid, obj in company_objs.items():
        created = parse_createdate(obj)
        print(f"    - ID {cid}, name '{company_name(cid)}', created {created.isoformat()}")

    if dry_run:
        print("  DRY RUN: no merges executed.")
        return success_count, failure_count, True, fuzzy_candidates_found, fuzzy_merge_performed, merged_pairs

    forward_ref_re = re.compile(r"forward reference to (\d+)")

    # Sort for deterministic behaviour
    all_ids_sorted = sorted(all_ids)

    for cid in all_ids_sorted:
        if cid == final_primary_id:
            continue

        src_name = company_name(cid)
        dst_name = company_name(final_primary_id)

        print(f"  Merging {cid} ('{src_name}') -> {final_primary_id} ('{dst_name}')")
        ok, info = merge_pair(session, headers, final_primary_id, cid)

        if not ok and "forward reference to" in info:
            match = forward_ref_re.search(info)
            if match:
                new_primary = match.group(1)
                if new_primary != final_primary_id:
                    print(
                        f"    Forward reference detected. Switching primary to {new_primary} and retrying."
                    )
                    final_primary_id = new_primary

                    # Ensure we have data for the new primary for name printing
                    if new_primary not in company_objs:
                        obj = fetch_company(
                            session,
                            headers,
                            new_primary,
                            props=["hs_canonical_object_id", "createdate", "name", "domain"],
                        )
                        if obj is not None:
                            company_objs[new_primary] = obj

                    dst_name = company_name(final_primary_id)
                    ok_retry, info_retry = merge_pair(
                        session, headers, final_primary_id, cid
                    )
                    if ok_retry:
                        print(f"    RESULT: OK (after primary switch) | {info_retry}")
                        success_count += 1
                        merged_pairs.append(f"{src_name} -> {dst_name}")
                    else:
                        print(f"    RESULT: FAIL (after primary switch) | {info_retry}")
                        failure_count += 1
                    time.sleep(sleep_seconds)
                    continue

        if ok:
            success_count += 1
            merged_pairs.append(f"{src_name} -> {dst_name}")
        else:
            failure_count += 1

        print(f"    RESULT: {'OK' if ok else 'FAIL'} | {info}")
        time.sleep(sleep_seconds)

    print("  Done.")
    return (
        success_count,
        failure_count,
        True,
        fuzzy_candidates_found,
        fuzzy_merge_performed,
        merged_pairs,
    )


def load_id_groups_from_file(path: str) -> Dict[str, Set[str]]:
    """
    Load groups of company IDs from a semicolon separated CSV.

    Supports:
      1) Fuzzy or name based manual review with an explicit id_list column:
         group_type;group_key;id_list;...

         where id_list = "123,456,789"

      2) company_merge.py manual_review format:
         group_type;group_key;primary_id;secondary_id;suggested_canonical_id;error

         where all three ID columns are combined into one cluster.
    """
    groups: Dict[str, Set[str]] = {}

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        if not reader.fieldnames:
            return {}

        fieldnames = [fn.strip() for fn in reader.fieldnames]
        has_id_list = "id_list" in fieldnames
        has_primary = "primary_id" in fieldnames
        has_secondary = "secondary_id" in fieldnames
        has_suggested = "suggested_canonical_id" in fieldnames

        for row in reader:
            group_key = (row.get("group_key") or "").strip()
            if not group_key:
                continue

            if group_key not in groups:
                groups[group_key] = set()

            # Case 1: explicit id_list from fuzzy/manual review
            if has_id_list:
                raw_ids = (row.get("id_list") or "").strip()
                if raw_ids:
                    for cid in raw_ids.split(","):
                        cid = cid.strip()
                        if cid:
                            groups[group_key].add(cid)

            # Case 2: manual_review from company_merge.py
            if has_primary or has_secondary or has_suggested:
                for col in ("primary_id", "secondary_id", "suggested_canonical_id"):
                    if col in row:
                        val = (row.get(col) or "").strip()
                        if val:
                            groups[group_key].add(val)

    # Remove empty groups
    return {k: v for k, v in groups.items() if v}


def merge_companies_for_id_group(
    session: requests.Session,
    headers: Dict[str, str],
    group_key: str,
    ids: Set[str],
    dry_run: bool,
    sleep_seconds: float = 0.3,
) -> Tuple[int, int, List[str]]:
    """
    Merge companies for a given group_key using an explicit set of IDs.

    Uses the same canonical resolution logic as merge_companies_for_name,
    but does not search by name. Instead it operates directly on known IDs.

    Returns:
      success_count
      failure_count
      merged_pairs -> list of "from_name -> to_name" strings
    """
    success_count = 0
    failure_count = 0
    merged_pairs: List[str] = []

    print(f"\n=== Group: {group_key} ===")

    if len(ids) <= 1:
        print("  Only one or zero IDs in this group. Nothing to merge.")
        return success_count, failure_count, merged_pairs

    # Fetch all company objects
    company_objs: Dict[str, Dict[str, Any]] = {}
    for cid in sorted(ids):
        obj = fetch_company(
            session,
            headers,
            cid,
            props=["hs_canonical_object_id", "createdate", "name", "domain"],
        )
        if obj is None:
            print(f"  WARNING: company {cid} not found, skipping.")
            continue
        company_objs[cid] = obj

    if len(company_objs) <= 1:
        print("  Only one valid company found in HubSpot for this group. Nothing to merge.")
        return success_count, failure_count, merged_pairs

    def company_name(company_id: str) -> str:
        obj = company_objs.get(company_id)
        if not obj:
            return company_id
        props = obj.get("properties", {}) or {}
        n = props.get("name") or ""
        return n if n else company_id

    # Resolve canonical IDs
    canonical_cache: Dict[str, str] = {}
    canonical_ids: Set[str] = set()

    for cid, obj in company_objs.items():
        props = obj.get("properties", {}) or {}
        canonical_id = resolve_canonical_id(
            session,
            headers,
            canonical_cache,
            cid,
            initial_properties=props,
        )
        canonical_ids.add(canonical_id)
        print(f"  Company {cid} ('{company_name(cid)}') -> canonical {canonical_id}")

    # Determine final primary canonical
    if len(canonical_ids) == 1:
        final_primary_id = next(iter(canonical_ids))
        print(f"  Single canonical for this group: {final_primary_id}")
    else:
        print(f"  Multiple canonical IDs found for this group: {', '.join(canonical_ids)}")
        canonical_list: List[Tuple[str, datetime]] = []
        for canon_id in canonical_ids:
            obj = fetch_company(
                session, headers, canon_id, props=["createdate", "name", "domain"]
            )
            if obj is None:
                created = datetime.max.replace(tzinfo=timezone.utc)
            else:
                created = parse_createdate(obj)
                company_objs.setdefault(canon_id, obj)
            canonical_list.append((canon_id, created))
            print(f"    Canonical candidate {canon_id}, created {created.isoformat()}")

        canonical_list.sort(key=lambda t: t[1])
        final_primary_id = canonical_list[0][0]
        print(
            f"  Selected final primary canonical ID {final_primary_id} (oldest createdate)."
        )

    print("  All candidate companies for this group:")
    for cid, obj in company_objs.items():
        created = parse_createdate(obj)
        print(
            f"    - ID {cid}, name '{company_name(cid)}', created {created.isoformat()}"
        )

    if dry_run:
        print("  DRY RUN: no merges executed.")
        return success_count, failure_count, merged_pairs

    forward_ref_re = re.compile(r"forward reference to (\d+)")

    # Sort IDs for deterministic behavior
    all_ids_sorted = sorted(company_objs.keys())

    for cid in all_ids_sorted:
        if cid == final_primary_id:
            continue

        src_name = company_name(cid)
        dst_name = company_name(final_primary_id)

        print(
            f"  Merging {cid} ('{src_name}') -> {final_primary_id} ('{dst_name}')"
        )
        ok, info = merge_pair(session, headers, final_primary_id, cid)

        if not ok and "forward reference to" in info:
            match = forward_ref_re.search(info)
            if match:
                new_primary = match.group(1)

                # Case A: secondary already canonically points to the current primary.
                # Example: trying 1579... -> 4633..., and error says:
                #   "objectId=1579... has a forward reference to 4633..."
                # In that situation the merge is redundant and can be treated as success.
                if new_primary == final_primary_id:
                    print(
                        "    Forward reference indicates that source already canonicalises "
                        f"to {final_primary_id}. Treating as merged."
                    )
                    success_count += 1
                    merged_pairs.append(f"{src_name} -> {dst_name}")
                    time.sleep(sleep_seconds)
                    continue

                # Case B: current primary is not canonical and needs to be switched.
                if new_primary != final_primary_id:
                    print(
                        f"    Forward reference detected. Switching primary to {new_primary} and retrying."
                    )
                    final_primary_id = new_primary

                    # Ensure we have data for the new primary for name printing
                    if new_primary not in company_objs:
                        obj = fetch_company(
                            session,
                            headers,
                            new_primary,
                            props=[
                                "hs_canonical_object_id",
                                "createdate",
                                "name",
                                "domain",
                            ],
                        )
                        if obj is not None:
                            company_objs[new_primary] = obj

                    dst_name = company_name(final_primary_id)
                    ok_retry, info_retry = merge_pair(
                        session, headers, final_primary_id, cid
                    )
                    if ok_retry:
                        print(f"    RESULT: OK (after primary switch) | {info_retry}")
                        success_count += 1
                        merged_pairs.append(f"{src_name} -> {dst_name}")
                    else:
                        print(f"    RESULT: FAIL (after primary switch) | {info_retry}")
                        failure_count += 1
                    time.sleep(sleep_seconds)
                    continue

        if ok:
            success_count += 1
            merged_pairs.append(f"{src_name} -> {dst_name}")
        else:
            failure_count += 1

        print(f"    RESULT: {'OK' if ok else 'FAIL'} | {info}")
        time.sleep(sleep_seconds)

    print("  Done.")
    return success_count, failure_count, merged_pairs


def collect_names_from_manual_review(path: str) -> List[str]:
    """
    Read CSV (manual_review or any similar) and collect
    unique names (group_key) for group_type == 'company_name'.

    Uses semicolon (;) as delimiter.
    """
    names: Set[str] = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if row.get("group_type") != "company_name":
                continue
            key = (row.get("group_key") or "").strip()
            if key:
                names.add(key)
    return sorted(names)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Merge HubSpot companies by name or ID groups.\n\n"
            "Usage examples:\n"
            "  - Merge by explicit name:\n"
            "      python merge_by_name.py --name \"Some Company\" --apply\n"
            "  - Merge groups from CSV (manual_review from company_merge.py or fuzzy):\n"
            "      python merge_by_name.py --file data/manual_review_*.csv --apply"
        )
    )
    parser.add_argument(
        "--name",
        action="append",
        help="Company name to merge. Can be given multiple times.",
    )
    parser.add_argument(
        "--file",
        dest="file",
        help=(
            "Path to CSV produced by company_merge.py (manual_review_*.csv) or "
            "fuzzy duplicates (with id_list column)."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute merges. Default is dry run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dry_run = not args.apply

    session, headers = get_session_and_headers()

    total_success = 0
    total_failure = 0
    all_merged_pairs: List[str] = []

    # 1. If file is given, first try ID based groups (manual_review from company_merge or fuzzy)
    if args.file:
        id_groups = load_id_groups_from_file(args.file)
        if id_groups:
            print(f"Loaded {len(id_groups)} groups from file (ID based).")
            if dry_run:
                print("DRY RUN mode. Use --apply to execute merges.")

            for group_key in sorted(id_groups.keys()):
                ids = id_groups[group_key]
                s, f, pairs = merge_companies_for_id_group(
                    session, headers, group_key, ids, dry_run=dry_run
                )
                total_success += s
                total_failure += f
                all_merged_pairs.extend(pairs)

            print("\nSummary (ID groups):")
            print(f"  Groups processed: {len(id_groups)}")
            print(f"  Merges successful: {total_success}")
            print(f"  Merges failed: {total_failure}")

            if all_merged_pairs:
                print("  Merged company name pairs:")
                for pair in all_merged_pairs:
                    print(f"    - {pair}")

            print("All done.")
            return

    # 2. Name based mode (either from --name or manual_review company_name rows)
    names: List[str] = []

    if args.name:
        names.extend(args.name)

    if args.file:
        auto_names = collect_names_from_manual_review(args.file)
        if auto_names:
            print(f"Loaded {len(auto_names)} names from file.")
        names.extend(auto_names)

    # Deduplicate while preserving order
    seen: Set[str] = set()
    unique_names: List[str] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            unique_names.append(n)

    if not unique_names:
        print("No names or ID groups to process.")
        return

    print(f"Processing {len(unique_names)} names.")
    if dry_run:
        print("DRY RUN mode. Use --apply to execute merges.")

    names_with_no_matches: List[str] = []
    names_with_fuzzy_candidates_skipped: List[str] = []
    names_with_fuzzy_merged: List[str] = []

    for name in unique_names:
        (
            s,
            f,
            had_any,
            fuzzy_found,
            fuzzy_merged,
            pairs,
        ) = merge_companies_for_name(
            session, headers, name, dry_run=dry_run
        )
        total_success += s
        total_failure += f
        all_merged_pairs.extend(pairs)

        if not had_any:
            names_with_no_matches.append(name)
        elif fuzzy_found and not fuzzy_merged:
            names_with_fuzzy_candidates_skipped.append(name)
        elif fuzzy_merged:
            names_with_fuzzy_merged.append(name)

    print("\nSummary (name based):")
    print(f"  Names processed: {len(unique_names)}")
    print(f"  Merges successful: {total_success}")
    print(f"  Merges failed: {total_failure}")

    if all_merged_pairs:
        print("  Merged company name pairs:")
        for pair in all_merged_pairs:
            print(f"    - {pair}")

    if names_with_no_matches:
        print("  Names with no HubSpot matches (exact or fuzzy):")
        for n in names_with_no_matches:
            print(f"    - {n}")

    if names_with_fuzzy_candidates_skipped:
        print("  Names where fuzzy candidates were found but not merged:")
        for n in names_with_fuzzy_candidates_skipped:
            print(f"    - {n}")

    if names_with_fuzzy_merged:
        print("  Names where fuzzy merge was performed after confirmation:")
        for n in names_with_fuzzy_merged:
            print(f"    - {n}")

    print("All done.")


if __name__ == "__main__":
    main()
