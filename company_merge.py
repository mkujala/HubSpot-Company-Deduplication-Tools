# file: company_merge.py
# Merge HubSpot companies from a CSV produced by the duplicate finder.
# Supports groups by domain, normalized company name, contact email domain,
# or an explicit 'group_key' column if present.
#
# Strategy:
# - Read all company IDs from CSV.
# - Fetch details (name, domain, createdate, hs_canonical_object_id).
# - For each group:
#     * Build canonical roots from hs_canonical_object_id.
#     * Choose one canonical root as final primary (oldest createdate).
#     * Merge other roots into primary.
#     * Merge all CSV IDs in the group into the same primary.
# - If an ID no longer exists in HubSpot, it is reported as SKIPPED_MISSING
#   and does not stop the script.
# - All actions are logged to logs/merge_log_YYYYMMDD-HHMMSS.csv.
# - If HubSpot returns "forward reference to X" errors, those cases are
#   collected into data/manual_review_YYYYMMDD-HHMMSS.csv for manual inspection.

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests
from dotenv import load_dotenv

BASE = "https://api.hubapi.com"
BATCH_READ = "/crm/v3/objects/companies/batch/read"
MERGE = "/crm/v3/objects/companies/merge"

MAX_BATCH = 100
TIMEOUT = 30

FORWARD_REF_RE = re.compile(r"forward reference to (\d+)")


def normalize_name(s: str) -> str:
    """Lowercase, collapse whitespace, strip common punctuation."""
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[\s\u00A0]+", " ", s)
    s = re.sub(r"[.,;:!?'\"()\\/\[\]{}&|]", "", s)
    s = s.strip()
    return s


def sniff_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(2048)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
        return dialect.delimiter
    except csv.Error:
        return ";"


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def http_request(session: requests.Session, method: str, url: str, headers: Dict, **kwargs) -> requests.Response:
    return session.request(method, url, timeout=TIMEOUT, headers=headers, **kwargs)


def batch_fetch_company_details(session: requests.Session, headers: Dict, ids: List[str]) -> Dict[str, Dict]:
    """
    Return mapping:
      id -> {
        "name": str,
        "domain": str,
        "createdate_raw": str or None,
        "createdate": datetime (naive UTC) or None,
        "canonical_id": str (hs_canonical_object_id or id)
      }

    If an id is invalid or already merged away, it simply does not appear
    in the returned mapping.
    """
    out: Dict[str, Dict] = {}
    for part in chunks(ids, MAX_BATCH):
        payload = {
            "idProperty": "hs_object_id",
            "properties": ["name", "domain", "createdate", "hs_canonical_object_id"],
            "inputs": [{"id": i} for i in part],
        }
        r = http_request(session, "POST", BASE + BATCH_READ, headers, json=payload)
        if r.status_code != 200:
            raise RuntimeError(f"Batch read error {r.status_code}: {r.text}")
        data = r.json()
        for row in data.get("results", []):
            cid = row["id"]
            props = row.get("properties", {}) or {}
            created_raw = props.get("createdate")
            created_dt = None
            if created_raw:
                try:
                    # parse as aware, convert to UTC, then drop tzinfo to get naive UTC
                    created_dt = (
                        datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                        .astimezone(timezone.utc)
                        .replace(tzinfo=None)
                    )
                except Exception:
                    created_dt = None
            canonical = props.get("hs_canonical_object_id") or cid
            out[cid] = {
                "name": props.get("name", ""),
                "domain": (props.get("domain") or "").lower().strip(),
                "createdate_raw": created_raw,
                "createdate": created_dt,
                "canonical_id": canonical,
            }
    return out


def merge_pair(
    session: requests.Session,
    headers: Dict,
    primary_id: str,
    secondary_id: str,
    dry_run: bool,
) -> Tuple[bool, str]:
    """
    Merge secondary into primary.

    Returns (ok, info_string).
    If secondary does not exist anymore, returns (False, "MISSING").
    """
    if primary_id == secondary_id:
        return True, "SAME_ID"

    if dry_run:
        return True, "DRY_RUN"

    payload = {"primaryObjectId": primary_id, "objectIdToMerge": secondary_id}
    r = http_request(session, "POST", BASE + MERGE, headers, json=payload)

    if r.status_code == 200:
        return True, "MERGED"
    if r.status_code in (404, 410):
        # Already merged away or not found
        return False, "MISSING"
    return False, f"error {r.status_code}: {r.text}"


def build_groups(rows: List[Dict[str, str]]) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    """
    Build groups keyed by (match_type, key_value) -> list(rows).

    Priority when choosing the key:
      1) if 'group_key' exists and not empty, use ('group', group_key)
      2) else if 'match_key' exists and not empty, use (match_type or 'match', match_key)
      3) else if match_type == company_name -> normalized 'name'
      4) else if match_type == contact_domain -> 'contact_domain'
      5) else if match_type == company_domain or empty -> 'domain'
      6) otherwise try to use domain or name as best effort.

    Only groups with > 1 row and non-empty key are kept.
    """
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)

    for r in rows:
        mt = (r.get("match_type") or "").strip()
        gid = (r.get("group_key") or "").strip()
        mkey = (r.get("match_key") or "").strip()

        domain = (r.get("domain") or "").lower().strip()
        name = r.get("name", "")
        contact_domain = (r.get("contact_domain") or "").lower().strip()

        if gid:
            key = ("group", gid.lower())
        elif mkey:
            key = (mt or "match", mkey.lower())
        elif mt == "company_name":
            key = ("company_name", normalize_name(name))
        elif mt == "contact_domain":
            key = ("contact_domain", contact_domain)
        elif mt == "company_domain" or mt == "":
            key = ("company_domain", domain)
        else:
            inner_key = domain or contact_domain or normalize_name(name)
            key = (mt, inner_key.lower() if inner_key else "")

        groups[key].append(r)

    pruned = {}
    for (mt, key), lst in groups.items():
        if key and len(lst) > 1:
            pruned[(mt, key)] = lst
    return pruned


def load_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    delim = sniff_delimiter(path)
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        if reader.fieldnames:
            reader.fieldnames = [h.lower() for h in reader.fieldnames]
        out = []
        for row in reader:
            out.append({(k.lower() if k else k): (v.strip() if isinstance(v, str) else v) for k, v in row.items()})
        return out


def main():
    parser = argparse.ArgumentParser(description="Merge HubSpot companies from a duplicate CSV.")
    parser.add_argument(
        "csv_path",
        help="Path to CSV with at least 'id' and one of: domain, name, contact_domain, or group_key.",
    )
    parser.add_argument("--apply", action="store_true", help="Perform merges (otherwise dry-run).")
    args = parser.parse_args()

    load_dotenv()
    token = os.getenv("HUBSPOT_TOKEN")
    if not token:
        print("Missing HUBSPOT_TOKEN in .env")
        sys.exit(2)

    rows = load_csv(args.csv_path)
    if not rows:
        print("CSV is empty")
        sys.exit(1)

    groups = build_groups(rows)
    if not groups:
        print("CSV does not contain any groups with more than one ID.")
        sys.exit(0)

    all_ids = sorted({r["id"] for r in rows if r.get("id")})
    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }

    details = batch_fetch_company_details(session, headers, all_ids)

    canonical_ids = set()
    for cid, info in details.items():
        canonical_ids.add(info["canonical_id"])

    missing_canonical = [cid for cid in canonical_ids if cid not in details]
    if missing_canonical:
        extra = batch_fetch_company_details(session, headers, missing_canonical)
        details.update(extra)

    # prepare logging
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = os.path.join("logs", f"merge_log_{ts}.csv")

    # manual review collection for forward reference cases
    manual_review_rows: List[Dict[str, str]] = []
    manual_seen = set()  # to dedupe (group_type, group_key, primary_id, secondary_id, canonical)

    total_merges_applied = 0
    total_merges_planned = 0

    with open(log_path, "w", newline="", encoding="utf-8") as log_f:
        log_writer = csv.writer(log_f, delimiter=";")
        log_writer.writerow(
            [
                "timestamp",
                "group_type",
                "group_key",
                "primary_id",
                "secondary_id",
                "action",
                "info",
            ]
        )

        def log_action(group_type: str, group_key: str, primary_id: str, secondary_id: str, action: str, info: str):
            log_writer.writerow(
                [
                    datetime.now().isoformat(timespec="seconds"),
                    group_type,
                    group_key,
                    primary_id,
                    secondary_id,
                    action,
                    info or "",
                ]
            )

        def add_manual_review_row(
            group_type: str,
            group_key: str,
            primary_id: str,
            secondary_id: str,
            canonical_id: str,
            error: str,
        ):
            """Append an issue to manual review CSV, de-duplicated by group + IDs + canonical."""
            key_tuple = (group_type, group_key, primary_id, secondary_id, canonical_id)
            if key_tuple in manual_seen:
                return
            manual_seen.add(key_tuple)
            manual_review_rows.append(
                {
                    "group_type": group_type,
                    "group_key": group_key,
                    "primary_id": primary_id,
                    "secondary_id": secondary_id,
                    "suggested_canonical_id": canonical_id,
                    "error": error,
                }
            )

        def collect_forward_reference(
            group_type: str,
            group_key: str,
            primary_id: str,
            secondary_id: str,
            info_str: str,
        ):
            """If info_str contains 'forward reference to X', add a row to manual review CSV."""
            m = FORWARD_REF_RE.search(info_str)
            if not m:
                return
            canonical_id = m.group(1)
            add_manual_review_row(
                group_type=group_type,
                group_key=group_key,
                primary_id=primary_id,
                secondary_id=secondary_id,
                canonical_id=canonical_id,
                error=info_str,
            )

        for (mt, key), lst in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
            ids_in_group = [r["id"] for r in lst if r.get("id")]

            canonical_for_id: Dict[str, str] = {}
            for cid in ids_in_group:
                info = details.get(cid)
                if not info:
                    canonical_for_id[cid] = cid
                else:
                    canonical_for_id[cid] = info["canonical_id"] or cid

            canonical_roots = sorted(set(canonical_for_id.values()))
            if not canonical_roots:
                print(f"=== Group: {mt} = {key} — no IDs with details, skipping.")
                continue

            def sort_key_for_primary(cid: str):
                info = details.get(cid, {})
                created = info.get("createdate")
                created_ord = created if created is not None else datetime.max  # all naive UTC
                try:
                    numeric_id = int(cid)
                except ValueError:
                    numeric_id = 10**20
                return (created_ord, numeric_id)

            primary_id = sorted(canonical_roots, key=sort_key_for_primary)[0]
            primary_info = details.get(primary_id, {})
            primary_created = primary_info.get("createdate_raw")
            primary_name = primary_info.get("name", "")

            print(f"=== Group: {mt} = {key} — {len(ids_in_group)} items")
            print(f"    canonical roots: {', '.join(canonical_roots)}")
            print(f"    primary: {primary_id} / '{primary_name}'")
            print(f"    primary created: {primary_created}")

            merged_secondaries = set()

            # First, merge other canonical roots into the primary
            for root_id in canonical_roots:
                if root_id == primary_id:
                    continue
                if root_id not in details:
                    msg = "no details returned by HubSpot"
                    print(f"    SKIPPED_MISSING_ROOT: {root_id} ({msg})")
                    log_action(mt, key, primary_id, root_id, "SKIPPED_MISSING_ROOT", msg)
                    add_manual_review_row(
                        group_type=mt,
                        group_key=key,
                        primary_id=primary_id,
                        secondary_id=root_id,
                        canonical_id=root_id,
                        error=msg,
                    )
                    continue
                ok, info_str = merge_pair(session, headers, primary_id, root_id, dry_run=(not args.apply))
                if not ok and info_str == "MISSING":
                    msg = "HubSpot returned not found"
                    print(f"    SKIPPED_MISSING_ROOT: {root_id} ({msg})")
                    log_action(mt, key, primary_id, root_id, "SKIPPED_MISSING_ROOT", msg)
                    add_manual_review_row(
                        group_type=mt,
                        group_key=key,
                        primary_id=primary_id,
                        secondary_id=root_id,
                        canonical_id=root_id,
                        error=msg,
                    )
                    continue
                action = "MERGED" if args.apply and ok else ("DRY_RUN" if not args.apply and ok else "SKIPPED")
                print(
                    f"    {action}: {root_id} -> {primary_id}"
                    f"{'' if info_str in ('MERGED', 'DRY_RUN', 'SAME_ID') else ' | ' + info_str}"
                )
                log_action(mt, key, primary_id, root_id, action, info_str)
                if not ok and info_str.startswith("error 400") and "forward reference to" in info_str:
                    collect_forward_reference(mt, key, primary_id, root_id, info_str)
                if ok:
                    merged_secondaries.add(root_id)
                    total_merges_planned += 1
                    if args.apply and info_str == "MERGED":
                        total_merges_applied += 1

            # Then, merge all CSV IDs into the same primary
            for cid in ids_in_group:
                if cid == primary_id or cid in merged_secondaries:
                    continue
                if cid not in details:
                    msg = "no details returned by HubSpot"
                    print(f"    SKIPPED_MISSING: {cid} ({msg})")
                    log_action(mt, key, primary_id, cid, "SKIPPED_MISSING", msg)
                    add_manual_review_row(
                        group_type=mt,
                        group_key=key,
                        primary_id=primary_id,
                        secondary_id=cid,
                        canonical_id="",
                        error=msg,
                    )
                    continue
                ok, info_str = merge_pair(session, headers, primary_id, cid, dry_run=(not args.apply))
                if not ok and info_str == "MISSING":
                    msg = "HubSpot returned not found"
                    print(f"    SKIPPED_MISSING: {cid} ({msg})")
                    log_action(mt, key, primary_id, cid, "SKIPPED_MISSING", msg)
                    add_manual_review_row(
                        group_type=mt,
                        group_key=key,
                        primary_id=primary_id,
                        secondary_id=cid,
                        canonical_id="",
                        error=msg,
                    )
                    continue
                action = "MERGED" if args.apply and ok else ("DRY_RUN" if not args.apply and ok else "SKIPPED")
                print(
                    f"    {action}: {cid} -> {primary_id}"
                    f"{'' if info_str in ('MERGED', 'DRY_RUN', 'SAME_ID') else ' | ' + info_str}"
                )
                log_action(mt, key, primary_id, cid, action, info_str)
                if not ok and info_str.startswith("error 400") and "forward reference to" in info_str:
                    collect_forward_reference(mt, key, primary_id, cid, info_str)
                if ok:
                    total_merges_planned += 1
                    if args.apply and info_str == "MERGED":
                        total_merges_applied += 1

            print()

    if args.apply:
        print(f"Done. Applied {total_merges_applied} merges across all groups.")
    else:
        print(f"Ready. Would merge {total_merges_planned} companies across groups (dry-run).")

    print(f"Log file written to: {log_path}")

    # write manual review CSV if needed
    if manual_review_rows:
        manual_path = os.path.join("data", f"manual_review_{ts}.csv")
        with open(manual_path, "w", newline="", encoding="utf-8") as mf:
            writer = csv.writer(mf, delimiter=";")
            writer.writerow(
                [
                    "group_type",
                    "group_key",
                    "primary_id",
                    "secondary_id",
                    "suggested_canonical_id",
                    "error",
                ]
            )
            for row in manual_review_rows:
                writer.writerow(
                    [
                        row["group_type"],
                        row["group_key"],
                        row["primary_id"],
                        row["secondary_id"],
                        row["suggested_canonical_id"],
                        row["error"],
                    ]
                )
        print(f"Manual review file written to: {manual_path}")
    else:
        print("No forward reference issues detected that require manual review.")


if __name__ == "__main__":
    main()
