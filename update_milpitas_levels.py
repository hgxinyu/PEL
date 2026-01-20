import argparse
import csv
from collections import defaultdict
from pathlib import Path
# This is used to update PAS Milpitas CSV files based on LevelUpdates.csv using the active flag.

def normalize(text: str) -> str:
    if text is None:
        return ""
    return " ".join(str(text).strip().split()).lower()


def subject_code(value: str) -> str:
    val = normalize(value)
    return val[:1].upper() if val else ""


def build_file_map(folder: Path) -> dict:
    return {p.stem.lower(): p for p in folder.glob("*.csv")}


def read_updates(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def find_column(fieldnames: list[str], target: str) -> str | None:
    lower_fields = {name.lower(): name for name in fieldnames}
    if target.lower() in lower_fields:
        return lower_fields[target.lower()]
    for name in fieldnames:
        if name.strip().lower() == target.lower():
            return name
    return None


def find_subject_column(fieldnames: list[str]) -> str | None:
    for name in fieldnames:
        if name.strip().lower().startswith("subject"):
            return name
    return None


def match_rows(
    rows: list[dict],
    subject_col: str,
    first_name_col: str | None,
    last_name_col: str | None,
    name_col: str | None,
    update_subject: str,
    update_name: str,
) -> list[int]:
    subj_code = subject_code(update_subject)
    name_norm = normalize(update_name)
    matches: list[int] = []

    for idx, row in enumerate(rows):
        row_subj = subject_code(row.get(subject_col, ""))
        if subj_code and row_subj != subj_code:
            continue

        if first_name_col and last_name_col:
            full = normalize(
                f"{row.get(first_name_col, '')} {row.get(last_name_col, '')}"
            )
            if full == name_norm:
                matches.append(idx)
                continue

            if " " not in name_norm:
                if normalize(row.get(first_name_col, "")) == name_norm:
                    matches.append(idx)
                    continue
                if normalize(row.get(last_name_col, "")) == name_norm:
                    matches.append(idx)
                    continue

        if name_col and normalize(row.get(name_col, "")) == name_norm:
            matches.append(idx)

    return matches


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Apply LevelUpdates.csv changes to PAS Milpitas CSV files "
            "by matching Subject + Name and updating PEL Wks. Level."
        )
    )
    parser.add_argument(
        "--updates",
        default="LevelUpdates.csv",
        help="Path to the updates CSV (default: LevelUpdates.csv).",
    )
    parser.add_argument(
        "--folder",
        default="PAS Milpitas CSV",
        help="Folder containing target CSV files (default: PAS Milpitas CSV).",
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        default=True,
        help="Apply only rows marked Active in the Active changes column (default).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Apply all rows (ignores Active changes).",
    )
    parser.add_argument(
        "--active-column",
        default="Active changes",
        help="Column name for Active flag (default: Active changes).",
    )
    parser.add_argument(
        "--active-value",
        default="Active",
        help="Value that marks a row Active (default: Active).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing files.",
    )
    args = parser.parse_args()

    updates_path = Path(args.updates)
    folder = Path(args.folder)

    if not updates_path.exists():
        raise SystemExit(f"Missing updates file: {updates_path}")
    if not folder.exists():
        raise SystemExit(f"Missing folder: {folder}")

    updates = read_updates(updates_path)
    if not updates:
        raise SystemExit("No updates found.")

    lower_header_map = {k.lower(): k for k in updates[0].keys()}
    required = ["subject", "name", "change to", "file name"]
    missing = [h for h in required if h not in lower_header_map]
    if missing:
        raise SystemExit(f"Missing columns in updates CSV: {', '.join(missing)}")

    active_only = args.active_only and not args.all
    if active_only:
        active_col_key = args.active_column.lower()
        if active_col_key not in lower_header_map:
            raise SystemExit(
                f"Missing Active column in updates CSV: {args.active_column}"
            )
        active_col = lower_header_map[active_col_key]
        active_value = normalize(args.active_value)
        updates = [
            row
            for row in updates
            if normalize(row.get(active_col, "")) == active_value
        ]
        if not updates:
            raise SystemExit("No active rows found.")

    file_map = build_file_map(folder)
    updates_by_file: dict[str | None, list[dict]] = defaultdict(list)
    for row in updates:
        file_name = row.get(lower_header_map["file name"], "").strip()
        if not file_name:
            updates_by_file[None].append(row)
            continue
        file_key = file_name[:-4].strip().lower() if file_name.lower().endswith(".csv") else file_name.lower()
        updates_by_file[file_key].append(row)

    missing_files: list[dict] = []
    missing_rows: list[dict] = []
    multiple_matches: list[dict] = []
    changed_files: list[str] = []
    updated_rows = 0

    for file_key, file_updates in updates_by_file.items():
        if file_key is None:
            for row in file_updates:
                missing_files.append({"file": "(blank)", "name": row.get(lower_header_map["name"], "")})
            continue

        path = file_map.get(file_key)
        if path is None:
            missing_files.append({"file": file_key, "count": len(file_updates)})
            continue

        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            if not fieldnames:
                continue
            rows = list(reader)

        level_col = find_column(fieldnames, "PEL Wks. Level")
        if not level_col:
            raise SystemExit(f"'PEL Wks. Level' column not found in {path.name}")

        subject_col = find_subject_column(fieldnames)
        if not subject_col:
            raise SystemExit(f"Subject column not found in {path.name}")

        lower_fields = {name.lower(): name for name in fieldnames}
        first_name_col = lower_fields.get("first name")
        last_name_col = lower_fields.get("last name")
        name_col = lower_fields.get("name")

        file_changed = False

        for update in file_updates:
            update_subject = update.get(lower_header_map["subject"], "")
            update_name = update.get(lower_header_map["name"], "")
            update_level = update.get(lower_header_map["change to"], "")

            matches = match_rows(
                rows,
                subject_col,
                first_name_col,
                last_name_col,
                name_col,
                update_subject,
                update_name,
            )

            if not matches:
                missing_rows.append(
                    {
                        "file": path.name,
                        "name": update_name,
                        "subject": update_subject,
                    }
                )
                continue

            if len(matches) > 1:
                multiple_matches.append(
                    {
                        "file": path.name,
                        "name": update_name,
                        "subject": update_subject,
                        "count": len(matches),
                    }
                )

            for idx in matches:
                if rows[idx].get(level_col) != update_level:
                    if not args.dry_run:
                        rows[idx][level_col] = update_level
                    updated_rows += 1
                    file_changed = True

        if file_changed and not args.dry_run:
            with path.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            changed_files.append(path.name)

    print(f"Active rows applied: {len(updates)}")
    print(f"Updated rows: {updated_rows}")
    print(f"Files changed: {len(changed_files)}")
    if missing_files:
        print("Missing files:")
        for item in missing_files:
            print(f"- {item}")
    if missing_rows:
        print("Rows not found:")
        for item in missing_rows:
            print(f"- {item}")
    if multiple_matches:
        print("Multiple matches (updated all):")
        for item in multiple_matches:
            print(f"- {item}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
