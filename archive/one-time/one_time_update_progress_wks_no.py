import os
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
            raise RuntimeError("Install psycopg (v3) or psycopg2 to use this script.") from exc


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

    progress_csv = base_dir / "progress_to_load.csv"
    if not progress_csv.exists():
        print(f"Missing CSV file: {progress_csv}")
        return 1

    apply_mode = os.environ.get("APPLY", "0") == "1"

    driver, conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE temp_progress_fix (
                    first_name text,
                    last_name text,
                    full_name text,
                    email text,
                    subject text,
                    pel_wks_level text,
                    lvs bigint,
                    pel_wks_no text,
                    progress_date date,
                    center text
                )
                """
            )
        conn.commit()

        copy_sql = (
            "COPY temp_progress_fix "
            "(first_name, last_name, full_name, email, subject, pel_wks_level, lvs, pel_wks_no, progress_date, center) "
            "FROM STDIN WITH (FORMAT csv, HEADER true)"
        )
        if driver == "psycopg":
            copy_csv_psycopg(conn, copy_sql, progress_csv)
        else:
            copy_csv_psycopg2(conn, copy_sql, progress_csv)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pel.progress dest
                JOIN temp_progress_fix src
                  ON dest.full_name IS NOT DISTINCT FROM src.full_name
                 AND dest.email IS NOT DISTINCT FROM src.email
                 AND dest.subject IS NOT DISTINCT FROM src.subject
                 AND dest.progress_date IS NOT DISTINCT FROM src.progress_date
                 AND dest.center IS NOT DISTINCT FROM src.center
                WHERE dest.pel_wks_no IS DISTINCT FROM src.pel_wks_no
                """
            )
            to_update = int(cur.fetchone()[0])

        print(f"Rows that would update pel_wks_no: {to_update}")

        if not apply_mode:
            print("Preview only. Re-run with APPLY=1 to apply updates.")
            conn.rollback()
            return 0

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pel.progress AS dest
                SET pel_wks_no = src.pel_wks_no
                FROM temp_progress_fix AS src
                WHERE dest.full_name IS NOT DISTINCT FROM src.full_name
                  AND dest.email IS NOT DISTINCT FROM src.email
                  AND dest.subject IS NOT DISTINCT FROM src.subject
                  AND dest.progress_date IS NOT DISTINCT FROM src.progress_date
                  AND dest.center IS NOT DISTINCT FROM src.center
                  AND dest.pel_wks_no IS DISTINCT FROM src.pel_wks_no
                """
            )
            updated = cur.rowcount
        conn.commit()
        print(f"Updated rows: {updated}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
