import os
import pandas as pd
import re

input_folder = "PAS Fremont CSV"
output_file = "Fremont students.csv"

all_data = []

for filename in os.listdir(input_folder):

    df = pd.read_csv(os.path.join(input_folder, filename))

    df = df.rename(columns=lambda c: c.strip())

    required_cols = [
        "First Name",
        "Last Name",
        "DOB (MM/DD/YY)",
        "Address",
        "Email",
        "DOE (Date of Enrollment MM/DD/YY)"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"Skipping {filename}, missing columns: {missing}")
        continue

    df = df[required_cols]

    df["First Name"] = df["First Name"].astype(str).str.strip()
    df["Last Name"] = df["Last Name"].astype(str).str.strip()
    df["Email"] = df["Email"].astype(str).str.strip().str.lower()

    all_data.append(df)


combined_df = pd.concat(all_data, ignore_index=True)

students_df = (
    combined_df
    .groupby("Email", as_index=False)
    .agg(lambda x: x.dropna().iloc[0] if not x.dropna().empty else pd.NA)
)

students_df = students_df[
    [
        "First Name",
        "Last Name",
        "DOB (MM/DD/YY)",
        "Address",
        "Email",
        "DOE (Date of Enrollment MM/DD/YY)"
    ]
]

students_df.to_csv(output_file, index=False)


