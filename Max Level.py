import pandas as pd
import os

input_folder = "PAS Freemont CSV"
output_file = "Freemont Highest.csv"


english_level = {
    "EK1":-4,"EK2":-3,"EK3":-2,"EK4":-1, "EK5":0,
    "EG1B":1, "EG2B":2, "EG1":3, "EG2":4, "EG3":5, "EG4":6,
    "EG5":7, "EG6":8, "EG7":9, "EG8":10, "EG9":11, "EG10":12,
    "EM1":13, "EM2":14, "EM3":15, "EM4":16,
    "EH1":17, "EH2":18, "EH3":19, "EH4":20, "EH5":21,
    "EH!":17,
}

math_level = {
    "MK1":-3, "MK2":-2, "MK3":-1, "MK4":0,
    "MG1":1, "MG2":2, "MG3":3, "MG4":4, "MG5":5,
    "MG6":6, "MG7":7, "MG8":8, "MG9":9, "MG10":10,
    "MM1":11, "MM2":12, "MM3":13, "MM4":14, "MM5":15,
    "MH1":16, "MH2":17, "MH3":18, "MH4":19, "MH5":20,
    "MH6":21, "MHG":22,"MHT":23, 
}

reverse_english_level = {v: k for k, v in english_level.items()}
reverse_math_level = {v: k for k, v in math_level.items()}

all_data = []

for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):
        all_data.append(pd.read_csv(os.path.join(input_folder, filename)))

combined_df = pd.concat(all_data, ignore_index=True)

combined_df["First Name"] = combined_df["First Name"].astype(str).str.strip()
combined_df["Last Name"] = combined_df["Last Name"].astype(str).str.strip()
combined_df["PEL Wks. Level"] = combined_df["PEL Wks. Level"].astype(str).str.strip().str.upper()

print(sorted(combined_df['PEL Wks. Level'].unique()))
def level_to_num(row):
    if row["Subject (M/E)"] == "E":
        return english_level.get(row["PEL Wks. Level"])
    if row["Subject (M/E)"] == "M":
        return math_level.get(row["PEL Wks. Level"])

combined_df["Level_num"] = combined_df.apply(level_to_num, axis=1)

result_df = (
    combined_df
    .groupby(
        ["First Name", "Last Name", "Subject (M/E)"],
        as_index=False
    )["Level_num"]
    .max()
)

def num_to_level(row):
    if row["Subject (M/E)"] == "E":
        return reverse_english_level.get(row["Level_num"])
    if row["Subject (M/E)"] == "M":
        return reverse_math_level.get(row["Level_num"])

result_df["PEL Wks. Level"] = result_df.apply(num_to_level, axis=1)

result_df = result_df.drop(columns="Level_num")

result_df.to_csv(output_file, index=False)
