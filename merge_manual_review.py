#!/usr/bin/env python
"""
Interactive manual-review merge for HubSpot company duplicates.

Usage examples:

  # Dry run (no merges, just preview and prompts)
  python manual_review_merge.py --file data/manual_review_*.csv

  # Execute merges after manual confirmation
  python manual_review_merge.py --file data/manual_review_*.csv --apply

This script is designed to consume the manual_review_*.csv that
company_merge.py writes. It reuses the merge engine and canonical
resolution logic from merge_by_name.py, but adds an interactive
confirmation step per group.
"""

import argparse
import sys
from typing import Dict, Set, List, Tuple

import requests

# We reuse the merge engine and helpers from merge_by_name.py
from merge_by_name import (
    get_session_and_headers,
    load_id_groups_from_file,
    merge_companies_for_id_group,
    fetch_company,
    parse_createdate,
)


def build_group_preview(
    session: requests.Session,
    headers: Dict[str, str],
    group_key: str,
    ids: Set[str],
) -> List[Tuple[str, str, str]]:
    """
    Fetch a small preview for all IDs in the group.

    Returns a list of tuples:
      (company_id, name_or_placeholder, createdate_iso_or_placeholder)
    """
    preview: List[Tuple[str, str, str]] = []

    for cid in sorted(ids):
        obj = fetch_company(
            session,
            headers,
            cid,
            props=["name", "domain", "createdate", "hs_canonical_object_id"],
        )
        if obj is None:
            preview.append((cid, "<missing in HubSpot>", "-"))
            continue

        props = obj.get("properties", {}) or {}
        name = (props.get("name") or "").strip() or "<no name>"
        created = parse_createdate(obj)
        created_iso = created.isoformat()
        preview.append((cid, name, created_iso))

    return preview


def prompt_user_for_group(
    group_key: str,
    preview_rows: List[Tuple[str, str, str]],
) -> str:
    """
    Print group summary and ask user what to do.

    Returns one of:
      "y"  -> merge this group
      "n"  -> skip this group
      "a"  -> merge this and all remaining groups without asking again
      "q"  -> abort processing
    """
    print(f"\n=== Manual review group: {group_key} ===")
    print("Companies in this group:")

    for cid, name, created_iso in preview_rows:
        print(f"  - ID {cid}, name '{name}', created {created_iso}")

    print()
    print("Actions:")
    print("  [y] merge this group")
    print("  [n] skip this group")
    print("  [a] merge this and all remaining groups without further prompts")
    print("  [q] quit without processing further groups")

    while True:
        choice = input("Your choice [y/n/a/q]: ").strip().lower()
        if choice in ("y", "n", "a", "q"):
            return choice
        print("Please answer with y, n, a or q.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Interactive manual-review merge for HubSpot companies.\n\n"
            "Typical use case:\n"
            "  1) Run company_merge.py which may write manual_review_*.csv\n"
            "  2) Run this script against that manual_review file to merge\n"
            "     selected groups after human confirmation."
        )
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to manual_review_*.csv (semicolon separated).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute merges. Default is dry run (no merges).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dry_run = not args.apply

    # Load groups of IDs from manual review CSV using existing helper.
    id_groups: Dict[str, Set[str]] = load_id_groups_from_file(args.file)
    if not id_groups:
        print(f"No ID groups found in {args.file}. Nothing to do.")
        sys.exit(0)

    print(f"Loaded {len(id_groups)} groups from {args.file}.")
    if dry_run:
        print("DRY RUN mode. Use --apply to execute merges.")

    session, headers = get_session_and_headers()

    total_success = 0
    total_failure = 0
    merge_all_remaining = False
    
    # Keep track of groups that still have failures, so they can be reviewed manually.
    error_groups: List[Tuple[str, int, int]] = []

    # Process groups in deterministic order
    for group_key in sorted(id_groups.keys()):
        ids = id_groups[group_key]
        if len(ids) <= 1:
            continue

        # Build and print preview for this group
        preview_rows = build_group_preview(session, headers, group_key, ids)

        if not preview_rows or all(row[1] == "<missing in HubSpot>" for row in preview_rows):
            print(f"\n=== Manual review group: {group_key} ===")
            print("All company IDs in this group are missing in HubSpot, skipping.")
            continue

        if not merge_all_remaining:
            choice = prompt_user_for_group(group_key, preview_rows)
            if choice == "n":
                print("Skipping this group.")
                continue
            if choice == "q":
                print("Aborting on user request.")
                break
            if choice == "a":
                merge_all_remaining = True
                print("Will merge this and all remaining groups without further prompts.")

        # Merge this group using existing merge engine
        s, f, pairs = merge_companies_for_id_group(
            session=session,
            headers=headers,
            group_key=group_key,
            ids=ids,
            dry_run=dry_run,
        )
        total_success += s
        total_failure += f

        if f:
            error_groups.append((group_key, s, f))

        if pairs:
            print("  Merged company name pairs in this group:")
            for p in pairs:
                print(f"    - {p}")

    print("\nSummary (manual review merge):")
    print(f"  Merges successful: {total_success}")
    print(f"  Merges failed:     {total_failure}")
    if dry_run:
        print("No merges were executed because DRY RUN mode was enabled.")
    else:
        print("Merges have been executed for the approved groups.")

    if error_groups:
        print("\nGroups with failed merges (manual follow-up recommended):")
        for g_key, s_cnt, f_cnt in error_groups:
            print(f"  - {g_key}: {s_cnt} successful, {f_cnt} failed")


if __name__ == "__main__":
    main()
