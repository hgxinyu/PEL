import os
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

    progress_columns = (
        "first_name, last_name, full_name, email, subject, pel_wks_level, lvs, pel_wks_no, progress_date, center"
    )
    copy_progress = (
        f"COPY temp_progress ({progress_columns}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )
    insert_progress = (
        f"INSERT INTO pel.progress ({progress_columns}) "
        "SELECT "
        "src.first_name, src.last_name, src.full_name, src.email, src.subject, "
        "src.pel_wks_level, src.lvs, src.pel_wks_no, src.progress_date, src.center "
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
        conn.commit()
    finally:
        conn.close()

    print("Progress CSV insert-only load complete.")
    print(f"CSV records processed: {total_rows}")
    print(f"CSV records after key dedupe: {dedup_rows}")
    print(f"Inserted records: {inserted}")
    print(f"Skipped existing records: {dedup_rows - inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
