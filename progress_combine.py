import os
import re
from datetime import datetime

import pandas as pd


MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def build_progress(output_folder: str) -> pd.DataFrame:
    rows = []

    for filename in os.listdir(output_folder):
        if not filename.endswith(".csv"):
            continue

        try:
            fname = filename.upper()

            month = None
            for key, value in MONTH_MAP.items():
                if key in fname:
                    month = value
                    break

            date_match = re.search(r"(\d{2})(\d{2})$", fname.replace(".CSV", ""))

            df = pd.read_csv(os.path.join(output_folder, filename))

            year = 2000 + int(date_match.group(2))
            file_date = datetime(year, month, 1)

            df["First Name"] = df["First Name"].astype(str).str.strip()
            df["Last Name"] = df["Last Name"].astype(str).str.strip()
            df["Email"] = df["Email"].astype(str).str.strip()
            df["PEL Wks. Level"] = df["PEL Wks. Level"].astype(str).str.strip().str.upper()

            df["Date"] = file_date
            rows.append(
                df[
                    [
                        "First Name",
                        "Last Name",
                        "Email",
                        "Subject (M/E)",
                        "PEL Wks. Level",
                        "PEL Wks. No.",
                        "Date",
                    ]
                ]
            )
        except KeyError as exc:
            print(filename, exc)

    progress_df = pd.concat(rows, ignore_index=True)

    return (
        progress_df.sort_values(["First Name", "Last Name", "Subject (M/E)", "Date"])
        .reset_index(drop=True)
    )


def main() -> int:
    fremont_progress = build_progress("Pas Fremont CSV")
    milpitas_progress = build_progress("Pas Milpitas CSV")

    #fremont_progress.to_csv("Fremont progress.csv", index=False)
    #milpitas_progress.to_csv("Milpitas progress.csv", index=False)

    fremont_progress["Center"] = "Fremont"
    milpitas_progress["Center"] = "Milpitas"

    combined = pd.concat([fremont_progress, milpitas_progress], ignore_index=True)

    combined = combined.rename(columns={"Subject (M/E)": "Subject"})
    combined["Subject"] = combined["Subject"].replace({"E": "English", "M": "Math"})
    if "Full Name" in combined.columns:
        combined = combined.drop(columns=["Full Name"])
    full_name = (
        combined["Last Name"].astype(str).str.strip()
        + ", "
        + combined["First Name"].astype(str).str.strip()
    )
    insert_at = combined.columns.get_loc("Last Name") + 1
    combined.insert(insert_at, "Full Name", full_name)

    worksheets = pd.read_csv("worksheets.csv")
    level_key = worksheets["PEL Wks. Level"].astype(str).str.strip()
    level_to_lvs = pd.Series(worksheets["Lvs Value"].values, index=level_key)
    combined_levels = combined["PEL Wks. Level"].astype(str).str.strip()
    lvs_values = combined_levels.map(level_to_lvs)
    if "lvs" in combined.columns:
        combined = combined.drop(columns=["lvs"])
    insert_at = combined.columns.get_loc("PEL Wks. Level") + 1
    combined.insert(
        insert_at, "lvs", pd.to_numeric(lvs_values, errors="coerce").astype("Int64")
    )

    combined.to_csv("progress.csv", index=False)

    #students_df1 = pd.read_csv("Fremont students.csv")
    #students_df2 = pd.read_csv("Milpitas students.csv")

    #students_df1["Center"] = "Fremont"
    #students_df2["Center"] = "Milpitas"

    #students_combined = pd.concat([students_df1, students_df2], ignore_index=True)
    #students_combined.to_csv("students.csv", index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
