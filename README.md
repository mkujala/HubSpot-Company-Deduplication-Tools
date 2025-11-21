# HubSpot Company Deduplication Toolkit

This repository contains a complete workflow for identifying, reviewing, and merging duplicate **HubSpot companies** safely and efficiently.

The toolkit supports:
- Exporting all canonical companies from HubSpot  
- Detecting duplicates using **exact matching**, **canonical chain analysis**, and **fuzzy matching**
- Generating manual review CSVs
- Automatically merging companies based on rules
- Interactive fuzzy merging with human confirmation

---

# 📌 Overview of the Full Workflow

Below is the recommended **end-to-end process** using all scripts in the repository.

---

## 1. Export All Canonical Companies  
**Script:** `export_all_companies.py`

Exports every company from HubSpot **but only the canonical master objects**, avoiding merged historical items.

**Command:**
```bash
python export_all_companies.py --limit 50
python export_all_companies.py          # full export (~20k companies)
```

Output example:
```
data/all_companies_20251121-113507.csv
```

---

## 2. Detect Deterministic Duplicates  
**Script:** `company_duplicates.py`

Finds duplicates using:
- exact name match  
- exact domain match  
- HubSpot "canonical roots"  

Produces a CSV of duplicates that are **safe to auto-merge**.

**Command:**
```bash
python company_duplicates.py --input data/all_companies_*.csv
```

Output example:
```
data/company_duplicates_2025-11-21.csv
```

---

## 3. Detect Fuzzy / Near-Duplicates  
**Script:** `company_duplicates_fuzzy.py`

Finds more complex duplicates using:
- token blocking
- domain weighting
- aggressive name normalization
- similarity scoring (threshold adjustable)

This script produces a CSV of **candidate fuzzy matches**.

**Command:**
```bash
python company_duplicates_fuzzy.py --input data/all_companies_*.csv --min-score 95
```

Output example:
```
data/company_duplicates_fuzzy_2025-11-21.csv
```

These are *candidates*, not confirmed duplicates.

---

## 4. Convert Fuzzy Matches to Mergeable ID-Clusters  
**Script:** `merge_fuzzy_ids.py`

Reads the fuzzy matches and forms **merge clusters** (groups of IDs belonging to the same company).

This script normalizes group keys, sanitizes results, and outputs a CSV that can be merged automatically.

**Command:**
```bash
python merge_fuzzy_ids.py --input data/company_duplicates_fuzzy_*.csv
```

Output example:
```
data/manual_review_from_fuzzy_2025-11-21.csv
```

---

## 5. Merge Companies by Name or Fuzzy Cluster  
**Script:** `merge_by_name.py`

This is the main merge engine.

It supports two paths:

### A) Merge deterministic name duplicates  
When manual review CSV contains multiple IDs for the same `group_key`:

```bash
python merge_by_name.py --file data/manual_review.csv --apply
```

### B) Merge fuzzy clusters  
When merge_fuzzy_ids produced clusters like:

```
group_key=“viking line”
ids=[6997014895, 20477930508]
```

Same command merges them.

Features:
- Automatically picks oldest canonical as primary
- Automatic retry when HubSpot returns “forward reference”
- Logs all merges
- Prints a summary like:
  ```
  Merged: Nokia Group <-> Nokia Oy
  ```

---

## 6. Merge Companies Using CSV From company_merge.py  
**Script:** `company_merge.py`

This script processes deterministic duplicate CSVs from `company_duplicates.py`.

It:
- Merges safely where HubSpot canonical chain allows it  
- Writes a `manual_review_xxx.csv` when user intervention is required  
- Does **not** automatically merge across canonical boundaries  

**Command:**
```bash
python company_merge.py data/dupes.csv --apply
```

---

# 📂 Files in Repository

| File | Purpose |
|------|---------|
| `.env` | Stores `HUBSPOT_TOKEN` |
| `company_duplicates.py` | Finds exact duplicates |
| `company_duplicates_fuzzy.py` | Finds fuzzy/near duplicates |
| `company_merge.py` | Merges deterministic duplicates |
| `merge_by_name.py` | Merges name-based or fuzzy clusters |
| `merge_fuzzy_ids.py` | Groups fuzzy matches into merge clusters |
| `export_all_companies.py` | Exports all canonical companies |
| `company_test.py` | Small test harness |
| `requirements.txt` | Python dependencies |
| `README.md` | Documentation |

---

# 🧠 How Merge Logic Works

### Deterministic merges
- Strict match by domain, name, or canonical root
- Safe → auto-merge

### Fuzzy merges
- The system uses:
  - Token similarity
  - Normalized company names
  - Domain similarity weighting
  - Removal of legal suffixes (Oy, AB, AS, GmbH…)
  - Removal of weak business suffixes (Group, Holding)
- Merge only after human confirmation

---

# 🧪 Interactive Fuzzy Merge (optional)
`merge_by_name.py` supports interactive fuzzy merge:

```
Fuzzy match detected:
  bluugo  <->  bluugo oy
Similarity: 97%

Merge? (y/n)
```

This prevents unintended merges.

---

# ✔ Recommended Workflow Summary

1. **Export all canonical companies**  
   → `export_all_companies.py`

2. **Find deterministic duplicates**  
   → `company_duplicates.py`

3. **Find fuzzy duplicates**  
   → `company_duplicates_fuzzy.py`

4. **Build merge clusters from fuzzy results**  
   → `merge_fuzzy_ids.py`

5. **Review & merge**  
   → `merge_by_name.py --apply`

6. **Finalize safe merges**  
   → `company_merge.py --apply`

---

# 📘 Example Environment Setup

`.env`:
```
HUBSPOT_TOKEN=your-token-here
```

Install dependencies:
```bash
pip install -r requirements.txt
```

---

If you want, I can add:

- ASCII workflow diagrams  
- A visual flowchart (PNG)  
- Example input/output CSVs  
- A troubleshooting chapter  
- Auto-fix instructions for common HubSpot API errors  
