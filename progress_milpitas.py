import pandas as pd
import os
import re
from datetime import datetime

output_folder = "Pas Milpitas CSV"
# output_folder = "Pas Fremont CSV"

month_map = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

reverse_month_map = {v: k for k, v in month_map.items()}

rows = []

for filename in os.listdir(output_folder):
    if not filename.endswith(".csv"):
        continue

    try:
        fname = filename.upper()

        month = None
        for m in month_map:
            if m in fname:
                month = month_map[m]
                break

        date_match = re.search(r"(\d{2})(\d{2})$", fname.replace(".CSV", ""))

        df = pd.read_csv(os.path.join(output_folder, filename))


        year = 2000 + int(date_match.group(2))
        file_date = datetime(year, month,1)


        df["First Name"] = df["First Name"].astype(str).str.strip()
        df["Last Name"] = df["Last Name"].astype(str).str.strip()
        df["Email"] = df["Email"].astype(str).str.strip()
        df["PEL Wks. Level"] = df["PEL Wks. Level"].astype(str).str.strip().str.upper()

        # df["Date"] = f"{reverse_month_map[month]} {(2000+int(date_match.group(2)))}"
        df['Date'] = file_date
        rows.append(
            df[
                [
                    "First Name",
                    "Last Name",
                    "Email",
                    "Subject (M/E)",
                    "PEL Wks. Level",
                    "PEL Wks. No.",
                    "Date"
                ]
            ]
        )
    except KeyError as e:
        print(filename, e)

# if not rows:
#     raise RuntimeError("No files matched expected filename format")

progress_df = pd.concat(rows, ignore_index=True)

progress_df = (
    progress_df
    .sort_values(["First Name", "Last Name", "Subject (M/E)", "Date"])
    .reset_index(drop=True)
)

progress_df.to_csv("Milpitas progress.csv", index=False)
