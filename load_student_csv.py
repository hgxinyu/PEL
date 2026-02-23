import csv
import os
from pathlib import Path
from typing import Optional

#  This script loads the student CSV file into the PostgreSQL database.


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


def resolve_students_csv(base_dir: Path) -> Optional[Path]:
    preferred = base_dir / "student_to_load.csv"
    if preferred.exists():
        return preferred
    fallback = base_dir / "student.csv"
    if fallback.exists():
        return fallback
    legacy = base_dir / "students.csv"
    if legacy.exists():
        return legacy
    return None


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

    students_csv = resolve_students_csv(base_dir)
    if not students_csv:
        print("Missing CSV file: student_to_load.csv, student.csv, or students.csv")
        return 1

    header = read_csv_header(students_csv)
    required_cols = {"Full Name", "Email"}
    missing_required = [col for col in required_cols if col not in header]
    if missing_required:
        print(f"Missing required columns in {students_csv.name}: {', '.join(sorted(missing_required))}")
        return 1
    header_to_db = {
        "First Name": "first_name",
        "Last Name": "last_name",
        "Full Name": "full_name",
        "DOB (MM/DD/YY)": "dob_raw",
        "Address": "address",
        "Tel:": "tel",
        "Tel": "tel",
        "Telephone": "tel",
        "Phone": "tel",
        "Phone Number": "tel",
        "Source": "source",
        "Email": "email",
        "DOE (Date of Enrollment MM/DD/YY)": "enrollment_date_raw",
        "Center": "center",
    }
    unknown_cols = [col for col in header if col not in header_to_db]
    if unknown_cols:
        print(f"Unknown columns in {students_csv.name}: {', '.join(unknown_cols)}")
        return 1

    db_columns = [header_to_db[col] for col in header]
    copy_students = (
        f"COPY temp_students ({', '.join(db_columns)}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )
    normalize_dates_sql = """
        UPDATE temp_students
        SET
            dob = CASE
                WHEN dob_raw IS NULL OR btrim(dob_raw) = '' THEN NULL
                WHEN dob_raw ~ '^\\d{4}-\\d{2}-\\d{2}(\\s+\\d{2}:\\d{2}:\\d{2})?$' THEN (dob_raw::timestamp)::date
                WHEN dob_raw ~ '^\\d{1,2}/\\d{1,2}/\\d{2}$' THEN to_date(dob_raw, 'MM/DD/YY')
                WHEN dob_raw ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(dob_raw, 'MM/DD/YYYY')
                ELSE NULL
            END,
            enrollment_date = CASE
                WHEN enrollment_date_raw IS NULL OR btrim(enrollment_date_raw) = '' THEN NULL
                WHEN enrollment_date_raw ~ '^\\d{4}-\\d{2}-\\d{2}(\\s+\\d{2}:\\d{2}:\\d{2})?$' THEN (enrollment_date_raw::timestamp)::date
                WHEN enrollment_date_raw ~ '^\\d{1,2}/\\d{1,2}/\\d{2}$' THEN to_date(enrollment_date_raw, 'MM/DD/YY')
                WHEN enrollment_date_raw ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(enrollment_date_raw, 'MM/DD/YYYY')
                ELSE NULL
            END
    """
    insert_db_columns = db_columns + [
        col for col in ["dob", "enrollment_date"] if col not in db_columns
    ]
    insert_columns = ", ".join(f"src.{col}" for col in insert_db_columns)
    insert_students = (
        f"INSERT INTO pel.students ({', '.join(insert_db_columns)}) "
        f"SELECT {insert_columns} "
        "FROM temp_students AS src "
        "WHERE NOT EXISTS ("
        "  SELECT 1 "
        "  FROM pel.students AS dest "
        "  WHERE dest.full_name IS NOT DISTINCT FROM src.full_name "
        "    AND dest.email IS NOT DISTINCT FROM src.email"
        ")"
    )
    dedup_temp_students = (
        "CREATE TEMP TABLE temp_students_dedup AS "
        "SELECT DISTINCT ON (full_name, email) * "
        "FROM temp_students "
        "ORDER BY full_name, email"
    )

    driver, conn = get_connection()
    try:
        execute_sql(conn, "CREATE TEMP TABLE temp_students (LIKE pel.students INCLUDING DEFAULTS)")
        if driver == "psycopg":
            copy_csv_psycopg(conn, copy_students, students_csv)
        else:
            copy_csv_psycopg2(conn, copy_students, students_csv)
        execute_sql(conn, normalize_dates_sql)
        total_rows = fetch_count(conn, "SELECT COUNT(*) FROM temp_students")
        execute_sql(conn, dedup_temp_students)
        dedup_rows = fetch_count(conn, "SELECT COUNT(*) FROM temp_students_dedup")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE temp_students")
            cur.execute("INSERT INTO temp_students SELECT * FROM temp_students_dedup")
            cur.execute(insert_students)
            inserted = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    print("Student CSV load complete.")
    print(f"CSV records processed: {total_rows}")
    print(f"CSV records after key dedupe: {dedup_rows}")
    print(f"Inserted records: {inserted}")
    print(f"Skipped existing records: {dedup_rows - inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
