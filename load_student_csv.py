import csv
import os
from pathlib import Path

#  This script loads the student CSV file into the PostgreSQL database.
WIPE_FIRST = True


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


def resolve_students_csv(base_dir: Path) -> Path | None:
    preferred = base_dir / "student.csv"
    if preferred.exists():
        return preferred
    fallback = base_dir / "students.csv"
    if fallback.exists():
        return fallback
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
        print("Missing CSV file: student.csv or students.csv")
        return 1

    header = read_csv_header(students_csv)
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
        f"COPY pel.students ({', '.join(db_columns)}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    driver, conn = get_connection()
    try:
        pre_count = fetch_count(conn, "SELECT COUNT(*) FROM pel.students")
        if WIPE_FIRST:
            execute_sql(conn, "TRUNCATE pel.students")
        if driver == "psycopg":
            copy_csv_psycopg(conn, copy_students, students_csv)
        else:
            copy_csv_psycopg2(conn, copy_students, students_csv)
        post_count = fetch_count(conn, "SELECT COUNT(*) FROM pel.students")
    finally:
        conn.close()

    deleted = pre_count if WIPE_FIRST else 0
    added = post_count if WIPE_FIRST else max(post_count - pre_count, 0)

    print("Student CSV load complete.")
    print(f"Deleted records: {deleted}")
    print(f"Added records: {added}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
