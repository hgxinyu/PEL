import os
import re
import pandas as pd


def _merge_columns(df: pd.DataFrame, candidates: list[str], target: str) -> pd.DataFrame:
    if not candidates:
        return df

    merged = df[candidates[0]].copy()
    for col in candidates[1:]:
        merged = merged.combine_first(df[col])
    df[target] = merged

    for col in candidates:
        if col != target:
            df = df.drop(columns=[col], errors="ignore")
    return df


def _canonicalize_progress_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: str(c).strip())

    alias_map = {
        "DOE            (Date of Enrollment MM/DD/YY)": "DOE (Date of Enrollment MM/DD/YY)",
        "Subject1 (M/E)": "Subject (M/E)",
    }
    df = df.rename(columns={k: v for k, v in alias_map.items() if k in df.columns})

    # Normalize any month/custom header variants like "DEC Wks. Level", "Jan Wks. No.", etc.
    def is_level_col(col: str) -> bool:
        c = str(col).upper()
        return bool(re.search(r"\bWKS?\b", c)) and ("LEVEL" in c or re.search(r"\bLV\b", c))

    def is_no_col(col: str) -> bool:
        c = str(col).upper()
        return bool(re.search(r"\bWKS?\b", c)) and ("NO" in c or "#" in c)

    level_cols = [c for c in df.columns if is_level_col(c)]
    no_cols = [c for c in df.columns if is_no_col(c)]

    if "PEL Wks. Level" in level_cols:
        level_cols = ["PEL Wks. Level"] + [c for c in level_cols if c != "PEL Wks. Level"]
    if "PEL Wks. No." in no_cols:
        no_cols = ["PEL Wks. No."] + [c for c in no_cols if c != "PEL Wks. No."]

    df = _merge_columns(df, level_cols, "PEL Wks. Level")
    df = _merge_columns(df, no_cols, "PEL Wks. No.")

    # If duplicate subject columns exist, keep first non-null row-wise.
    subject_cols = [c for c in df.columns if c == "Subject (M/E)" or c.startswith("Subject (M/E).")]
    df = _merge_columns(df, subject_cols, "Subject (M/E)")

    return df


def turn_into_csv(folder_path, output_folder):

    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx") :
            print("processing file: "+filename)

            file_path = os.path.join(folder_path, filename)

            df = pd.read_excel(file_path, sheet_name="S")
            df = df.iloc[3:]


            csv_filename = filename.replace(".xlsx", ".csv")
            csv_path = os.path.join(output_folder, csv_filename)

            df.to_csv(csv_path, index=False,header=False)


def clean_csv_files(folder_path, nan_threshold):

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            

            df = pd.read_csv(file_path)
            df = _canonicalize_progress_columns(df)

            columns_to_drop = [col for col in df.columns if pd.isna(col) or 'Unnamed:' in str(col)]
            df.drop(columns=columns_to_drop, inplace=True)

            # Fallback for unexpected historical files with shifted headers.
            if "PEL Wks. Level" not in df.columns and len(df.columns) > 10:
                df = df.rename(columns={df.columns[10]: "PEL Wks. Level"})
            if "PEL Wks. No." not in df.columns and len(df.columns) > 11:
                df = df.rename(columns={df.columns[11]: "PEL Wks. No."})


            cutoff_index = None

            for i, row in df.iterrows():
                if row.isna().sum() > nan_threshold:
                    cutoff_index = i
                    break

            if cutoff_index is not None:
                df = df.iloc[:cutoff_index]
            

            df.to_csv(file_path, index=False)
            print(f"Processed {filename}")

folder_path = "PAS Fremont"
output_folder = "PAS Fremont CSV"

folder_path1 = "PAS Milpitas"
output_folder1 = "PAS Milpitas CSV"

#turn_into_csv(folder_path, output_folder)
#clean_csv_files(output_folder, nan_threshold=10)

turn_into_csv(folder_path1, output_folder1)
clean_csv_files(output_folder1, nan_threshold=10)

folders = ["PAS Fremont CSV", "PAS Milpitas CSV"]

for folder_path in folders:
    for filename in os.listdir(folder_path):
        if "DEC" in filename.upper():

            digit = filename[-5]
            if digit.isdigit():
                if digit == "0":
                    new_filename = filename[:-6] + "19"

                else:
                    last_digit = int(digit)
                    new_last_digit = str(last_digit - 1)
                    new_filename = filename[:-5] + new_last_digit
            old_path = os.path.join(folder_path, filename)
            new_path = os.path.join(folder_path, new_filename+".csv")
            os.rename(old_path, new_path)
            print(f"Renamed {filename} to {new_filename}")
