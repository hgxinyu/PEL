import os
import sys
from pathlib import Path


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


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    load_dotenv(base_dir / ".env")

    if "DATABASE_URL" not in os.environ:
        print("DATABASE_URL is not set. Put it in .env or set it in your shell.")
        return 1

    students_csv = base_dir / "students.csv"
    progress_csv = base_dir / "progress.csv"

    missing = [str(p) for p in [students_csv, progress_csv] if not p.exists()]
    if missing:
        print("Missing CSV files:")
        for path in missing:
            print(f"  - {path}")
        return 1

    copy_students = (
        "COPY pel.students (first_name, last_name, dob_raw, address, email, enrollment_date_raw) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )
    copy_progress = (
        "COPY pel.progress (first_name, last_name, email, subject, pel_wks_level, pel_wks_no, progress_date) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    driver, conn = get_connection()
    try:
        if driver == "psycopg":
            copy_csv_psycopg(conn, copy_students, students_csv)
            copy_csv_psycopg(conn, copy_progress, progress_csv)
        else:
            copy_csv_psycopg2(conn, copy_students, students_csv)
            copy_csv_psycopg2(conn, copy_progress, progress_csv)
    finally:
        conn.close()

    print("CSV load complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())