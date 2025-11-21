#!/usr/bin/env python
import argparse
import csv
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()
HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")

BASE_URL = "https://api.hubapi.com"


# ----------------------------------------------------------------------
# HTTP helper
# ----------------------------------------------------------------------
def hubspot_request(method: str, path: str, **kwargs) -> requests.Response:
    url = BASE_URL + path
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {HUBSPOT_TOKEN}"
    headers["Content-Type"] = "application/json"
    resp = requests.request(method, url, headers=headers, **kwargs)
    return resp


# ----------------------------------------------------------------------
# Union-find (disjoint set) for building ID clusters
# ----------------------------------------------------------------------
class UnionFind:
    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
            return x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: str, b: str) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra != rb:
            self.parent[rb] = ra

    def groups(self) -> Dict[str, Set[str]]:
        groups: Dict[str, Set[str]] = defaultdict(set)
        for x in list(self.parent.keys()):
            root = self.find(x)
            groups[root].add(x)
        return groups


# ----------------------------------------------------------------------
# Read fuzzy CSV and build clusters
# ----------------------------------------------------------------------
def build_clusters_from_fuzzy(fuzzy_path: Path) -> List[Set[str]]:
    """
    Read company_duplicates_fuzzy_*.csv and form ID clusters
    from (id1, id2) pairs using union-find.

    Expected CSV columns (semicolon-delimited):
        id1;name1;domain1;normalized_name1;id2;name2;domain2;normalized_name2;score;block_type;block_key
    """
    uf = UnionFind()

    with fuzzy_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            id1 = (row.get("id1") or "").strip()
            id2 = (row.get("id2") or "").strip()
            if not id1 or not id2:
                continue
            uf.union(id1, id2)

    groups_dict = uf.groups()
    clusters = [ids for ids in groups_dict.values() if len(ids) > 1]
    return clusters


# ----------------------------------------------------------------------
# HubSpot company helpers
# ----------------------------------------------------------------------
def fetch_company_info(company_id: str) -> Tuple[str, datetime]:
    """
    Fetch company name and createdate for a given ID.

    Returns (name, createdate). If createdate is missing,
    datetime.max is returned so that this ID will not be chosen
    as the earliest one.
    """
    resp = hubspot_request(
        "GET",
        f"/crm/v3/objects/companies/{company_id}",
        params={"properties": "name,createdate"},
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch company {company_id}: {resp.status_code} {resp.text}"
        )

    data = resp.json()
    props = data.get("properties", {}) or {}

    name = props.get("name") or f"Company {company_id}"
    created_raw = props.get("createdate") or props.get("hs_createdate")

    if created_raw:
        created = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
    else:
        created = datetime.max

    return name, created


def merge_companies(
    primary_id: str, secondary_id: str, apply: bool
) -> Tuple[bool, str, str]:
    """
    Try to merge secondary -> primary.

    Returns (success, new_primary_id, message).

    If HubSpot returns a forward reference error, new_primary_id is the
    canonical ID extracted from the error message, otherwise it is primary_id.
    """
    if primary_id == secondary_id:
        return True, primary_id, "skip same id"

    if not apply:
        # Dry run: do not call the API, but pretend success for logging
        return True, primary_id, "dry-run, not merged"

    payload = {
        "primaryObjectId": primary_id,
        "objectIdToMerge": secondary_id,
    }
    resp = hubspot_request(
        "POST",
        "/crm/v3/objects/companies/merge",
        json=payload,
    )

    if resp.status_code == 200:
        return True, primary_id, "merged"

    # Inspect for forward reference
    msg = ""
    try:
        data = resp.json()
        msg = data.get("message", "")
    except Exception:
        msg = resp.text

    m = re.search(r"forward reference to (\d+)", msg)
    if resp.status_code == 400 and m:
        new_primary = m.group(1)
        return False, new_primary, f"forward_ref:{new_primary}"

    return False, primary_id, f"HTTP {resp.status_code}: {msg}"


# ----------------------------------------------------------------------
# Cluster processing
# ----------------------------------------------------------------------
def process_cluster(
    cluster_ids: Set[str],
    apply: bool,
    merged_pairs: List[Tuple[str, str, str, str]],
) -> None:
    """
    Process a single cluster of company IDs.

    - Fetch name + createdate for each ID
    - Choose initial primary as the oldest createdate
    - Merge others into primary, handling forward references
    - On successful merge (in apply mode), append pair to merged_pairs
      as (secondary_id, primary_id, secondary_name, primary_name)
    """
    ids = sorted(cluster_ids)
    print(f"\nCluster with {len(ids)} companies: {', '.join(ids)}")

    # Fetch info for each ID
    info_map: Dict[str, Tuple[str, datetime]] = {}
    for cid in ids:
        try:
            name, created = fetch_company_info(cid)
            info_map[cid] = (name, created)
            print(f"  ID {cid}: {name} | created {created.isoformat()}")
        except Exception as e:
            print(f"  ID {cid}: FAILED to fetch info: {e}")

    if len(info_map) < 2:
        print("  Not enough valid companies in this cluster, skipping.")
        return

    # Choose oldest as initial primary
    primary = min(info_map.items(), key=lambda kv: kv[1][1])[0]
    primary_name = info_map[primary][0]
    print(f"  Initial primary (oldest) will be {primary} ({primary_name})")

    for cid in ids:
        if cid == primary:
            continue

        sec_name = info_map.get(cid, (f"Company {cid}", datetime.max))[0]
        print(f"  Merging {cid} ({sec_name}) -> {primary} ({primary_name})")

        ok, new_primary, info = merge_companies(primary, cid, apply)
        print(f"    RESULT: {info}")

        if not ok and info.startswith("forward_ref:"):
            # Switch primary to canonical from error, retry once
            print(
                f"    Forward reference detected, switching primary to {new_primary} and retrying"
            )
            primary = new_primary
            # Try to fetch name for the new primary if not already present
            if primary not in info_map:
                try:
                    p_name, p_created = fetch_company_info(primary)
                    info_map[primary] = (p_name, p_created)
                except Exception as e:
                    print(f"    FAILED to fetch info for new primary {primary}: {e}")
            primary_name = info_map.get(primary, (f"Company {primary}", datetime.max))[0]

            ok2, new_primary2, info2 = merge_companies(primary, cid, apply)
            print(f"      RETRY RESULT: {info2}")
            if ok2:
                if apply:
                    merged_pairs.append(
                        (cid, primary, sec_name, primary_name)
                    )
                primary = new_primary2
                primary_name = info_map.get(
                    primary, (f"Company {primary}", datetime.max)
                )[0]
        else:
            # No forward reference; if merge succeeded and we are applying, record pair
            if ok and apply:
                merged_pairs.append((cid, primary, sec_name, primary_name))


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge fuzzy duplicate companies by ID clusters."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to fuzzy duplicates CSV (company_duplicates_fuzzy_*.csv)",
    )
    parser.add_argument(
        "--max-clusters",
        type=int,
        default=None,
        help="Optional limit for number of clusters to process (for testing).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually perform merges. Without this flag runs as dry run.",
    )

    args = parser.parse_args()
    fuzzy_path = Path(args.file)

    if not fuzzy_path.exists():
        raise SystemExit(f"Fuzzy file does not exist: {fuzzy_path}")

    clusters = build_clusters_from_fuzzy(fuzzy_path)
    print(f"Found {len(clusters)} clusters with size >= 2.")

    if args.max_clusters is not None:
        clusters = clusters[: args.max_clusters]
        print(f"Limiting to first {len(clusters)} clusters due to --max-clusters.")

    merged_pairs: List[Tuple[str, str, str, str]] = []

    for cluster in clusters:
        process_cluster(cluster, apply=args.apply, merged_pairs=merged_pairs)

    if args.apply:
        print("\nSummary of successful merges:")
        if not merged_pairs:
            print("  No successful merges recorded.")
        else:
            for sec_id, prim_id, sec_name, prim_name in merged_pairs:
                print(f"  {sec_name} <-> {prim_name} ({sec_id} -> {prim_id})")
    else:
        print("\nDry run complete. No merges were applied. Use --apply to perform merges.")


if __name__ == "__main__":
    main()
