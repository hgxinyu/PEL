import os
import csv
from pathlib import Path
# This script loads the combined progress CSV file into the PostgreSQL database.

def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key not in os.environ:
            os.environ[key] = value


def get_connection():
    try:
        import psycopg  # type: ignore

        return "psycopg", psycopg.connect(os.environ["DATABASE_URL"])
    except ModuleNotFoundError:
        try:
            import psycopg2  # type: ignore

            return "psycopg2", psycopg2.connect(os.environ["DATABASE_URL"])
        except ModuleNotFoundError as exc:
            raise RuntimeError("Install psycopg (v3) or psycopg2 to use this loader.") from exc


def copy_csv_psycopg(conn, sql: str, csv_path: Path) -> None:
    with conn.cursor() as cur:
        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            with cur.copy(sql) as copy:
                while True:
                    chunk = handle.read(8192)
                    if not chunk:
                        break
                    copy.write(chunk)
    conn.commit()


def copy_csv_psycopg2(conn, sql: str, csv_path: Path) -> None:
    with conn.cursor() as cur:
        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            cur.copy_expert(sql, handle)
    conn.commit()


def execute_sql(conn, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fetch_count(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def read_csv_header(csv_path: Path) -> list[str]:
    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
    if not header:
        raise ValueError(f"Missing header row in {csv_path}")
    return [col.strip() for col in header]


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    load_dotenv(base_dir / ".env")

    if "DATABASE_URL" not in os.environ:
        print("DATABASE_URL is not set. Put it in .env or set it in your shell.")
        return 1

    progress_csv = base_dir / "progress_to_load.csv"
    if not progress_csv.exists():
        progress_csv = base_dir / "progress.csv"

    missing = [str(progress_csv)] if not progress_csv.exists() else []
    if missing:
        print("Missing CSV files:")
        for path in missing:
            print(f"  - {path}")
        return 1

    header = read_csv_header(progress_csv)
    header_to_db = {
        "First Name": "first_name",
        "Last Name": "last_name",
        "Full Name": "full_name",
        "Email": "email",
        "Subject": "subject",
        "PEL Wks. Level": "pel_wks_level",
        "PEL Wks. No.": "pel_wks_no",
        "Date": "progress_date",
        "Center": "center",
        "lvs": "lvs",
        "Notes": "notes",
        "Student ID": "student_id",
    }
    required_cols = {
        "First Name",
        "Last Name",
        "Full Name",
        "Email",
        "Subject",
        "PEL Wks. Level",
        "PEL Wks. No.",
        "Date",
        "Center",
        "lvs",
    }
    missing_required = [col for col in required_cols if col not in header]
    if missing_required:
        print(f"Missing required columns in {progress_csv.name}: {', '.join(sorted(missing_required))}")
        return 1
    unknown_cols = [col for col in header if col not in header_to_db]
    if unknown_cols:
        print(f"Unknown columns in {progress_csv.name}: {', '.join(unknown_cols)}")
        return 1

    progress_columns = ", ".join(header_to_db[col] for col in header)
    copy_progress = (
        f"COPY temp_progress ({progress_columns}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )
    insert_progress = (
        f"INSERT INTO pel.progress ({progress_columns}) "
        "SELECT "
        + ", ".join(f"src.{header_to_db[col]}" for col in header)
        + " "
        "FROM temp_progress AS src "
        "WHERE NOT EXISTS ("
        "  SELECT 1 "
        "  FROM pel.progress AS dest "
        "  WHERE dest.full_name IS NOT DISTINCT FROM src.full_name "
        "    AND dest.email IS NOT DISTINCT FROM src.email "
        "    AND dest.subject IS NOT DISTINCT FROM src.subject "
        "    AND dest.progress_date IS NOT DISTINCT FROM src.progress_date "
        "    AND dest.center IS NOT DISTINCT FROM src.center"
        ")"
    )
    dedup_temp_progress = (
        "CREATE TEMP TABLE temp_progress_dedup AS "
        "SELECT DISTINCT ON (full_name, email, subject, progress_date, center) * "
        "FROM temp_progress "
        "ORDER BY full_name, email, subject, progress_date, center, lvs DESC NULLS LAST, pel_wks_no DESC NULLS LAST"
    )

    driver, conn = get_connection()
    try:
        execute_sql(conn, "ALTER TABLE pel.progress ADD COLUMN IF NOT EXISTS notes text")
        execute_sql(conn, "CREATE TEMP TABLE temp_progress (LIKE pel.progress INCLUDING DEFAULTS)")
        if driver == "psycopg":
            copy_csv_psycopg(conn, copy_progress, progress_csv)
        else:
            copy_csv_psycopg2(conn, copy_progress, progress_csv)
        total_rows = fetch_count(conn, "SELECT COUNT(*) FROM temp_progress")
        execute_sql(conn, dedup_temp_progress)
        dedup_rows = fetch_count(conn, "SELECT COUNT(*) FROM temp_progress_dedup")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE temp_progress")
            cur.execute("INSERT INTO temp_progress SELECT * FROM temp_progress_dedup")
            cur.execute(insert_progress)
            inserted = cur.rowcount
            cur.execute(
                """
                UPDATE pel.progress AS p
                SET student_id = s.student_id::text
                FROM pel.students AS s
                WHERE p.student_id IS NULL
                  AND p.full_name IS NOT DISTINCT FROM s.full_name
                  AND p.email IS NOT DISTINCT FROM s.email
                """
            )
            linked_student_id = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    print("Progress CSV insert-only load complete.")
    print(f"CSV records processed: {total_rows}")
    print(f"CSV records after key dedupe: {dedup_rows}")
    print(f"Inserted records: {inserted}")
    print(f"Skipped existing records: {dedup_rows - inserted}")
    print(f"Progress rows linked to student_id: {linked_student_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
