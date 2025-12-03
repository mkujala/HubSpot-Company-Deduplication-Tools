# HubSpot Company Duplicate Management Toolkit

This toolkit provides a structured and safe workflow for identifying and merging duplicate companies in HubSpot.  
The process handles deterministic duplicates, forward-reference canonical cases, and fuzzy-match duplicates in a controlled sequence.  
Some scripts act only as libraries and are not meant to be executed directly.

---

## Full Workflow Overview

```
company_duplicates.py
    ↓
company_merge.py
    ↓
merge_manual_review.py
    ↓
export_all_companies.py
    ↓
company_duplicates_fuzzy.py
    ↓
merge_fuzzy_ids.py   (uses merge_by_name.py as a library)
```

---

## 1. Deterministic duplicate detection  
**Script:** `company_duplicates.py`

This script finds duplicates using reliable rules:

- domain  
- normalized company name  
- contact-derived email domain  
- business_id (custom property)

Options:  
  --no-by-domain *– disable domain-based matching (default: enabled)*  
  --no-by-name *– disable name-based matching (default: enabled)*  
  --no-by-contact-domain *– disable contact-domain matching (default: enabled)*  

**Output file:**

```
duplicates_YYYYMMDD-HHMMSS.csv
```

**Columns:**

```
id;domain;name;business_id;match_type;match_key
```

---

## 2. Automatic merging of deterministic duplicates  
**Script:** `company_merge.py`

Takes the duplicate CSV from step 1 and performs safe merges.

Option:  
  --apply *- without --apply script makes dry run, and does not change anything in hubspot*

### Behaviors:

- groups rows by `(match_type, match_key)`  
- identifies canonical candidates using createdate  
- performs merges when safe  
- logs forward-reference conflicts into a file- 

```
manual_review_YYYYMMDD-HHMMSS.csv
```

### Example:

```
python company_merge.py data/duplicates_*.csv --apply
```

---

## 3. Manual review of forward-reference cases  
**Script:** `merge_manual_review.py`

Some merges cannot be completed automatically due to HubSpot’s canonical chain.  
This script guides the user through those cases interactively.

### Example:

```
python merge_manual_review.py --file data/manual_review_*.csv --apply
```

### Features:

- fetches real-time HubSpot data for each company  
- displays preview (name, domain, business_id, canonical_id, createdate)  
- asks: merge / skip / merge all remaining / quit  
- attempts merges safely  
- prints a summary of groups that still contain unresolved conflicts  

---

## 4. Export full company list for fuzzy analysis  
**Script:** `export_all_companies.py`

Run after deterministic + manual merges.

Options:  
  --output, -o *- (optional) Output path; default: data/all_companies_<timestamp>.csv*  
  --include-merged-history *- (optional, default: false)*  
  --max-count *- (optional, default: None), max count of companies*  
  --limit *- (optional, default: 100), API page size = how many companies fetched per request*  

### Example:

```
python export_all_companies.py
```

### Output:

```
all_companies_YYYYMMDD-HHMMSS.csv
```

---

## 5. Fuzzy duplicate detection (optional)  
**Script:** `company_duplicates_fuzzy.py`

### Example

```
python company_duplicates_fuzzy.py --input data/all_companies_*.csv
```
all options:
```
python company_duplicates_fuzzy.py \
  --input data/all_companies_20251202-XXXXXX.csv \
  --output data/company_duplicates_fuzzy_test.csv \  
  --min-score 90 \
  --max-bucket-size 200 \
  --max-pairs 200
```
Defaults:  
  min-score 90  
  max-bucket-size 200  
  max-pairs no-limit  

### Output:

```
company_duplicates_fuzzy_*.csv
```

**Columns:**  
```
id1;id2;score;reason
```

---

## 6. Fuzzy cluster merging  
**Script:** `merge_fuzzy_ids.py`  
Uses **merge_by_name.py** as a library — **never run merge_by_name.py directly**.

Options:  
  --file *- (required) – fuzzy CSV path*  
  --max-clusters *- (optional, default: None) – limit how many clusters to process*  
  --apply *- (optional, default: dry run) – actually perform merges*  

### Example:

```
python merge_fuzzy_ids.py --file data/company_duplicates_fuzzy_*.csv --apply
```

---

## Module Roles

### `company_duplicates.py`  
Deterministic duplicate detection.

### `company_merge.py`  
Automatic safe merging of deterministic duplicates.

### `merge_manual_review.py`  
Interactive merge approval for forward-reference canonical cases.

### `export_all_companies.py`  
Exports all companies for fuzzy analysis.

### `company_duplicates_fuzzy.py`  
Finds fuzzy duplicate candidates.

### `merge_fuzzy_ids.py`  
Merges fuzzy clusters; relies on merge_by_name.py.

### `merge_by_name.py`  
**Library for canonical logic and merging.**  
Not executed directly.

---

## Recommended Complete Workflow

```
python company_duplicates.py
python company_merge.py --apply
python merge_manual_review.py --apply
python export_all_companies.py
python company_duplicates_fuzzy.py
python merge_fuzzy_ids.py --apply
```

---

## Notes

- Forward reference messages come from HubSpot’s canonical system and are expected in some cases.  
- No merges happen unless `--apply` is used.  
- The workflow is idempotent and safe to rerun.  
