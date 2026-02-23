# PEL Data Load Workflow (PAS Raw -> `pel.students` / `pel.progress`)

This document is the standard process to load new PAS data into the DB using **insert-only** behavior (no updates/deletes on existing DB rows).

## 1) What this pipeline does

- Source files: Excel files in `PAS Raw/` (Fremont + Milpitas)
- Review files:
  - `student_to_load.csv`
  - `progress_to_load.csv`
- DB loaders (insert-only):
  - `load_student_csv.py`
  - `load_progress_csv.py`

Current loader behavior:
- `pel.students`: insert if `(full_name, email)` does not already exist
- `pel.progress`: insert if `(full_name, email, subject, progress_date, center)` does not already exist
- `pel.progress`: if present, `Notes` from CSV is loaded into `pel.progress.notes`
- `pel.progress`: `student_id` is auto-linked from `pel.students` by `(full_name, email)` after insert
- `pel.students` date handling for new rows:
  - raw text still goes to `dob_raw` / `enrollment_date_raw`
  - parsed date goes to `dob` / `enrollment_date`
  - supported input formats: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, `MM/DD/YY`, `MM/DD/YYYY`

## 2) Prerequisites

- Python 3.9+
- `.env` file in repo root with:

```env
DATABASE_URL=postgres://...
```

- Packages:

```bash
python3 -m pip install pandas openpyxl psycopg2-binary
```

(If you use `psycopg` v3, that also works.)

## 3) Monthly process

### Step A: Put new raw files in `PAS Raw/`

Expected naming pattern (example):
- `PAS FREMONT JAN 021226.xlsx`
- `PAS MILPITAS JAN 021226.xlsx`

### Step B: Reprocess raw files into `*_to_load.csv`

Use the established reprocessing flow to regenerate:
- `student_to_load.csv`
- `progress_to_load.csv`

Rules used in this project:
- only records from the target month (example: Jan 2026 -> `2026-01-01` in progress)
- only records not already in DB
- known name cleanup is applied (example: normalize `Ivaan SInghal` -> `Ivaan Singhal`)
- header aliases are normalized before combining (for example `DEC Wks. Level/No.` -> `PEL Wks. Level/No.`)
- any header containing `Wks` + (`Lv`/`Level`) maps to `PEL Wks. Level`
- any header containing `Wks` + (`#`/`No`) maps to `PEL Wks. No.`

### Step C: Review before load

Open and review:
- `student_to_load.csv`
- `progress_to_load.csv`

Recommended quick checks:

```bash
python3 - <<'PY'
import pandas as pd
s = pd.read_csv('student_to_load.csv')
p = pd.read_csv('progress_to_load.csv')
print('students:', len(s))
print('progress:', len(p))
print('progress dates:', sorted(p['Date'].astype(str).unique().tolist()))
PY
```

## 4) Backup DB tables before loading

Create CSV backups of current DB tables:

```bash
python3 - <<'PY'
import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import psycopg2

base = Path('.').resolve()
for line in (base / '.env').read_text().splitlines():
    if '=' in line and not line.strip().startswith('#'):
        k,v = line.split('=',1)
        os.environ.setdefault(k.strip(), v.strip())

conn = psycopg2.connect(os.environ['DATABASE_URL'])
stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
pd.read_sql_query('SELECT * FROM pel.students', conn).to_csv(f'backup_pel_students_{stamp}.csv', index=False)
pd.read_sql_query('SELECT * FROM pel.progress', conn).to_csv(f'backup_pel_progress_{stamp}.csv', index=False)
conn.close()
print('backup complete')
PY
```

## 5) Load into DB (insert-only)

```bash
python3 load_student_csv.py
python3 load_progress_csv.py
```

Expected output includes:
- `CSV records processed`
- `CSV records after key dedupe`
- `Inserted records`
- `Skipped existing records`

## 6) File roles

- `student_to_load.csv` and `progress_to_load.csv`: active load inputs
- `archive/student.csv` and `archive/progress.csv`: legacy outputs kept only for history

## 7) Troubleshooting

- `DATABASE_URL is not set`: check `.env`
- Missing package errors: install required Python packages
- If row counts look wrong: stop and review `*_to_load.csv` before running loaders
- Do not run any script that truncates `pel.progress`; current loader is insert-only
