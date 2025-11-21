#!/usr/bin/env python
import argparse
import csv
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

from rapidfuzz import fuzz


# Common non-informative words we want to ignore when checking overlap
STOPWORDS: Set[str] = {
    "the",
    "of",
    "and",
    "or",
    "for",
    "in",
    "at",
    "by",

    # legal forms (already mostly handled in normalize_name, but kept for safety)
    "oy",
    "oyj",
    "ab",
    "as",
    "gmbh",
    "ltd",
    "inc",
    "sa",
    "spa",
    "nv",
    "bv",
    "srl",
    "company",
    "co",
    "group",

    # generic institution words
    "university",
    "universitetet",
    "universitet",
    "college",
    "school",
    "academy",
    "akademi",
    "institute",
    "instituutti",
    "institutet",
}


def normalize_name(name: str) -> str:
    """
    Normalize company name for fuzzy comparison.

    Normalization steps:
      - return empty string for None/empty input
      - lowercase
      - trim alusta ja lopusta ja tiivistä useat välilyönnit yhdeksi
      - poista yleisimmät yhtiömuoto-suffiksit lopusta (oy, oyj, ab, as, gmbh, ltd, inc, sa, nv, bv, srl)
      - poista lopusta lisäksi "heikot" suffiksit, kuten "group", jotka eivät yleensä
        ole olennaisia duplikaattien tunnistamisessa
    """
    if not name:
        return ""

    # Lowercase ja turhien välilyöntien poisto.
    # Esim. "  Oulun   Kuivaustekniikka   Group Oy  " -> "oulun kuivaustekniikka group oy"
    s = name.strip().lower()
    tokens = s.split()  # pilkotaan sanoiksi

    # Yleisimmät yhtiömuodot, jotka poistetaan NIMEN LOPUSTA, jos ne esiintyvät
    # viimeisenä tokenina. 'spa' jätetään pois, koska se voi olla osa brändiä
    # (esim. "ikaalinen spa") eikä italialainen yhtiömuoto.
    legal_suffixes = {
        "oy",
        "oyj",
        "ab",
        "as",
        "gmbh",
        "ltd",
        "inc",
        "sa",
        "nv",
        "bv",
        "srl",
    }

    # Poista kaikki peräkkäiset yhtiömuotosuffiksit lopusta.
    # Esim. ["oulun", "kuivaustekniikka", "group", "oy"] -> ["oulun", "kuivaustekniikka", "group"]
    while tokens and tokens[-1] in legal_suffixes:
        tokens.pop()

    # "Heikommat" suffiksit, jotka ovat usein vain lisämääreitä brändin perässä,
    # eivätkä estä duplikaattien tunnistusta. Esim. "X Group" vs "X".
    weak_suffixes = {
        "group",
    }

    # Poista peräkkäiset heikot suffiksit lopusta.
    # Esim. ["oulun", "kuivaustekniikka", "group"] -> ["oulun", "kuivaustekniikka"]
    while tokens and tokens[-1] in weak_suffixes:
        tokens.pop()

    # Kootaan normalisoitu nimi takaisin yhdeksi merkkijonoksi.
    # Esim. ["oulun", "kuivaustekniikka"] -> "oulun kuivaustekniikka"
    return " ".join(tokens)

def first_token(normalized_name: str) -> str:
    """
    Return the first token of a normalized name.
    """
    if not normalized_name:
        return ""
    return normalized_name.split()[0]


def significant_tokens(normalized_name: str) -> Set[str]:
    """
    Return a set of 'significant' tokens from a normalized name,
    i.e. tokens not in STOPWORDS.
    """
    if not normalized_name:
        return set()
    tokens = normalized_name.split()
    return {t for t in tokens if t not in STOPWORDS}


def has_significant_token_overlap(norm1: str, norm2: str) -> bool:
    """
    Check whether two normalized names share at least one non-stopword token.
    This helps to avoid high scores for names like
    'university of the arts helsinki' vs 'university of oslo library'.
    """
    sig1 = significant_tokens(norm1)
    sig2 = significant_tokens(norm2)
    if not sig1 or not sig2:
        # If either side has no significant tokens, be conservative and require equality
        return norm1 == norm2
    return len(sig1.intersection(sig2)) > 0


def extract_domain_root(domain: str) -> str:
    """
    Extract a simple domain root from a full domain, trying to approximate
    the registered domain (second-level):
      audionova.dk            -> audionova
      ttt-teatteri.fi         -> ttt-teatteri
      no.experis.com          -> experis
      example.co.uk           -> example
    """
    if not domain:
        return ""
    domain = domain.strip().lower()
    parts = domain.split(".")
    if len(parts) == 1:
        return parts[0]

    # Handle very common two-level public suffixes like co.uk
    tld = parts[-1]
    sld = parts[-2]
    if tld == "uk" and sld in {"co", "ac", "gov", "org"}:
        if len(parts) >= 3:
            return parts[-3]
        return sld

    # Default: use the second-last label as root
    return sld



def domain_root_similarity(root1: str, root2: str) -> Optional[float]:
    """
    Compute similarity between two domain roots using WRatio.
    If either root is empty, return None (no domain-based decision).
    """
    if not root1 or not root2:
        return None
    return float(fuzz.WRatio(root1, root2))


def load_companies(
    path: str,
) -> List[Dict[str, Any]]:
    """
    Load companies from a semicolon-delimited CSV exported by export_all_companies.py.
    Expected columns:
        id;name;domain;createdate;hs_canonical_object_id;resolved_canonical_id;is_canonical
    """
    companies: List[Dict[str, Any]] = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cid = (row.get("id") or "").strip()
            if not cid:
                continue

            name = (row.get("name") or "").strip()
            domain = (row.get("domain") or "").strip()

            norm_name = normalize_name(name)
            token = first_token(norm_name)
            domain_lower = domain.strip().lower()

            companies.append(
                {
                    "id": cid,
                    "name": name,
                    "domain": domain,
                    "normalized_name": norm_name,
                    "first_token": token,
                    "domain_lower": domain_lower,
                }
            )

    print(f"Loaded {len(companies)} companies from {path}")
    return companies


def build_buckets(
    companies: List[Dict[str, Any]],
    max_bucket_size: int,
) -> Dict[str, List[int]]:
    """
    Build blocking buckets using:
      - first token of normalized name (token:<token>)
      - domain (domain:<domain>)
    Values in buckets are indices into the companies list.
    Buckets larger than max_bucket_size are kept but will be skipped later.
    """
    buckets: Dict[str, List[int]] = {}

    for idx, c in enumerate(companies):
        token = c["first_token"]
        domain_lower = c["domain_lower"]

        if token:
            key = f"token:{token}"
            buckets.setdefault(key, []).append(idx)

        if domain_lower:
            key = f"domain:{domain_lower}"
            buckets.setdefault(key, []).append(idx)

    print(f"Built {len(buckets)} buckets from first token and domain.")
    large_buckets = sum(1 for b in buckets.values() if len(b) > max_bucket_size)
    if large_buckets:
        print(
            f"Warning: {large_buckets} buckets have more than {max_bucket_size} companies "
            f"and will be skipped during pair generation."
        )

    return buckets


def generate_pairs(
    companies: List[Dict[str, Any]],
    buckets: Dict[str, List[int]],
    min_score: float,
    max_bucket_size: int,
    max_pairs: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Generate fuzzy duplicate candidate pairs using WRatio.
    Only pairs with:
      - significant token overlap AND
      - score >= min_score AND
      - pass domain-root heuristics
    are returned.
    Uses buckets to limit comparisons.
    """
    pairs: List[Dict[str, Any]] = []
    seen_pairs: Set[Tuple[str, str]] = set()

    for bucket_key, indices in buckets.items():
        n = len(indices)
        if n < 2:
            continue

        if n > max_bucket_size:
            print(
                f"Skipping bucket {bucket_key} with size {n} (> {max_bucket_size})."
            )
            continue

        block_type = "token" if bucket_key.startswith("token:") else "domain"

        for i in range(n):
            idx1 = indices[i]
            c1 = companies[idx1]
            id1 = c1["id"]
            name1 = c1["name"]
            domain1 = c1["domain"]
            norm1 = c1["normalized_name"]

            for j in range(i + 1, n):
                idx2 = indices[j]
                c2 = companies[idx2]
                id2 = c2["id"]
                name2 = c2["name"]
                domain2 = c2["domain"]
                norm2 = c2["normalized_name"]

                if id1 == id2:
                    continue

                pair_key = (id1, id2) if id1 < id2 else (id2, id1)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                # Require significant token overlap, otherwise skip early
                if not has_significant_token_overlap(norm1, norm2):
                    continue

                # Compute WRatio similarity between normalized names
                score = float(fuzz.WRatio(norm1, norm2))
                if score < min_score:
                    continue

                # Domain-root heuristics: only applied if both sides have domain roots
                root1 = extract_domain_root(domain1)
                root2 = extract_domain_root(domain2)
                root_score = domain_root_similarity(root1, root2)

                # Only apply domain check when names are not identical.
                # Identical normalized names (e.g. "audionova" vs "audionova")
                # are allowed regardless of domain root.
                if norm1 != norm2 and root_score is not None:
                    # Length-based penalty: big difference in root length lowers trust
                    length_diff = abs(len(root1) - len(root2))
                    adjusted_root_score = root_score - length_diff * 5.0

                    # Strong domain disagreement: discard
                    if adjusted_root_score < 60.0:
                        continue

                    # Moderate disagreement + not extremely high name score: discard
                    if adjusted_root_score < 80.0 and score < 98.0:
                        continue

                pair = {
                    "id1": id1,
                    "name1": name1,
                    "domain1": domain1,
                    "normalized_name1": norm1,
                    "id2": id2,
                    "name2": name2,
                    "domain2": domain2,
                    "normalized_name2": norm2,
                    "score": f"{score:.1f}",
                    "block_type": block_type,
                    "block_key": bucket_key,
                }
                pairs.append(pair)

                if max_pairs is not None and len(pairs) >= max_pairs:
                    print(
                        f"Max pairs limit {max_pairs} reached during generation. "
                        f"Stopping early."
                    )
                    return pairs

    print(f"Generated {len(pairs)} candidate pairs with score >= {min_score}.")
    return pairs


def write_pairs_csv(path: str, pairs: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    fieldnames = [
        "id1",
        "name1",
        "domain1",
        "normalized_name1",
        "id2",
        "name2",
        "domain2",
        "normalized_name2",
        "score",
        "block_type",
        "block_key",
    ]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in pairs:
            writer.writerow(row)

    print(f"Wrote {len(pairs)} pairs to {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect fuzzy duplicate candidate pairs from exported canonical companies. "
            "Reads CSV from export_all_companies.py and outputs fuzzy duplicate pairs."
        )
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Input CSV path from export_all_companies.py (semicolon-delimited).",
    )
    parser.add_argument(
        "--output",
        "-o",
        help=(
            "Output CSV path for fuzzy duplicate pairs. "
            "Default: data/company_duplicates_fuzzy_<timestamp>.csv"
        ),
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=90.0,
        help="Minimum WRatio score (0–100) to include a pair. Default: 90.0",
    )
    parser.add_argument(
        "--max-bucket-size",
        type=int,
        default=200,
        help="Maximum bucket size for pair generation. Larger buckets are skipped. Default: 200",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        help="Optional maximum number of pairs to generate (for safe testing).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = args.input
    if args.output:
        output_path = args.output
    else:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_path = os.path.join("data", f"company_duplicates_fuzzy_{ts}.csv")

    companies = load_companies(input_path)
    if not companies:
        print("No companies loaded. Exiting.")
        return

    buckets = build_buckets(companies, max_bucket_size=args.max_bucket_size)

    pairs = generate_pairs(
        companies=companies,
        buckets=buckets,
        min_score=args.min_score,
        max_bucket_size=args.max_bucket_size,
        max_pairs=args.max_pairs,
    )

    write_pairs_csv(output_path, pairs)


if __name__ == "__main__":
    main()
