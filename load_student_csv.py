import os
from pathlib import Path


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


def resolve_students_csv(base_dir: Path) -> Path | None:
    preferred = base_dir / "student.csv"
    if preferred.exists():
        return preferred
    fallback = base_dir / "students.csv"
    if fallback.exists():
        return fallback
    return None


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

    copy_students = (
        "COPY pel.students (first_name, last_name, full_name, dob_raw, address, email, enrollment_date_raw, center) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    driver, conn = get_connection()
    try:
        if WIPE_FIRST:
            execute_sql(conn, "TRUNCATE pel.students")
        if driver == "psycopg":
            copy_csv_psycopg(conn, copy_students, students_csv)
        else:
            copy_csv_psycopg2(conn, copy_students, students_csv)
    finally:
        conn.close()

    print("Student CSV load complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
