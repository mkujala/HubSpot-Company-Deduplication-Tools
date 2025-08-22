# HubSpot Company Deduplication Tools

This repository contains Python scripts to **identify, analyze, and merge duplicate company records in HubSpot** via the official CRM API.  
All scripts use a HubSpot **Private App token** stored securely in a `.env` file.

---

## 📦 Scripts Overview

### 1. `company_duplicates.py`
Purpose: **Detect and export duplicate companies by domain.**

- Fetches all companies via the HubSpot API.  
- Normalizes domains (lowercase, strip `www.`, punycode support).  
- Groups companies by domain.  
- Identifies groups where multiple companies share the same domain.  
- Writes all duplicate records to `data/duplicates_YYYYMMDD-HHMMSS.csv`.  

Output CSV columns:
```
id;domain;name
```

Usage:
```bash
python company_duplicates.py
```
Result example:
```
✅ Saved 145 rows to data/duplicates_20250822-134500.csv
```

---

### 2. `company_merge.py`
Purpose: **Safely merge duplicate companies into a single “primary” record.**

- Reads a semicolon-separated CSV (format: `id;domain;name`)  
- Groups records by domain.  
- Fetches metadata (`name`, `domain`, `hs_createdate`, `createdate`).  
- Selects a **primary record** per domain:  
  - The oldest `createdate` if available, otherwise the numerically smallest ID.  
- Resolves **canonical company IDs** (handles HubSpot’s alias / forward references).  
- Merges duplicates into the chosen primary via HubSpot’s **merge endpoint**.  
- Writes a **log file** with all actions: `logs/merge_log_YYYYMMDD-HHMMSS.csv`.

Log CSV columns:
```
domain;primary_id;primary_name;primary_created_raw;mergee_id;mergee_name;mergee_created_raw;status
```

Two modes:
- **Dry-run (default)**: No changes in HubSpot, only prints plan and logs as `DRY_RUN`.
- **Apply mode (`--apply`)**: Actually performs merges, logs as `MERGED`.

Usage:
```bash
# Dry-run (safe to test)
python company_merge.py ./data/duplicates.csv

# Apply changes (performs merges in HubSpot)
python company_merge.py ./data/duplicates.csv --apply
```

---

### 3. `company_test.py`
Purpose: **Test connectivity to the HubSpot API.**

- Loads token from `.env`.  
- Fetches a single company with `name` and `domain`.  
- Confirms whether the token is valid and API access works.

Usage:
```bash
python company_test.py
```

Output examples:
```
✅ Connection works!
First company: {'id': '123456789', 'properties': {'name': 'Example Oy', 'domain': 'example.com'}}
```
or
```
❌ Error: 401 {"message":"Invalid authentication credentials"}
```

---

## ⚙️ Setup Instructions

1. **Clone the repository**
```bash
git clone https://github.com/<your-org>/<repo-name>.git
cd <repo-name>
```

2. **Create and activate a Python environment**
```bash
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```
Dependencies:
- `requests`
- `python-dotenv`
- `idna` (optional, for robust domain normalization)

4. **Configure environment**
Create a `.env` file in the project root:
```
HUBSPOT_TOKEN=pat-xxxxxx-your-private-app-token
```

⚠️ `.env` is ignored by `.gitignore`. Never commit your token to GitHub.

---

## 🗂 Repository Structure

```
.
├── company_duplicates.py   # Detect duplicates by domain and export to CSV
├── company_merge.py        # Merge duplicates into a single primary, with logging
├── company_test.py         # Connectivity test script
├── data/                   # Duplicate exports (gitignored, only .gitkeep committed)
│   └── .gitkeep
├── logs/                   # Merge logs (gitignored, only .gitkeep committed)
│   └── .gitkeep
├── .env_example            # Example environment file
├── .gitignore              # Ignores sensitive and generated files
└── README.md               # This file
```

---

## 🔒 Safety Notes

- Always **run `company_merge.py` without `--apply` first** to verify which records would be merged.  
- The merge operation in HubSpot is **irreversible**. Once merged, a company cannot be split back.  
- All associated records (contacts, deals, tickets) of merged companies are reassigned to the primary.  
- Primary company’s property values take precedence. Some multi-value fields may merge automatically.  

---

## 🚀 Typical Workflow

1. Detect duplicates:
```bash
python company_duplicates.py
```
→ Produces `data/duplicates_YYYYMMDD-HHMMSS.csv`

2. Review CSV manually (optional).  

3. Dry-run merge plan:
```bash
python company_merge.py ./data/duplicates_YYYYMMDD-HHMMSS.csv
```

4. Apply merges:
```bash
python company_merge.py ./data/duplicates_YYYYMMDD-HHMMSS.csv --apply
```

5. Check log file in `logs/` for audit trail.

---

## 🧪 Example Dry-Run Output

```
=== Domain: example.com — 3 records (primary: 12345 / 'Example Oy')
    created: 2020-02-12 10:00:00+00:00 (raw: 2020-02-12T10:00:00Z)
    merge candidates: 2
    DRY_RUN: 67890 -> 12345
    DRY_RUN: 54321 -> 12345

Completed. Would merge 2 records in 1 domain group.
📄 Log saved: logs/merge_log_20250822-101200.csv
```

---

## 📜 License

MIT License. See [LICENSE](LICENSE) for details.
