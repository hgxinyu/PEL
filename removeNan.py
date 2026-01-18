# import os
# import pandas as pd

# folder = "PAS Milpitas CSV"

# NAN_THRESHOLD = 10

# for filename in os.listdir(folder):
#     if not filename.endswith(".csv"):
#         continue

#     file_path = os.path.join(folder, filename)
#     df = pd.read_csv(file_path)

#     cutoff_index = None

#     for i, row in df.iterrows():
#         if row.isna().sum() > NAN_THRESHOLD:
#             cutoff_index = i
#             break

#     if cutoff_index is not None:
#         df = df.iloc[:cutoff_index]

#     output_path = os.path.join(folder, filename)
#     df.to_csv(output_path, index=False)



import os
import pandas as pd

# Path to the folder containing CSV files
folder_path = "PAS Milpitas CSV"

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        
        # Read the CSV
        df = pd.read_csv(file_path)
        
        # Remove columns with NaN headers or headers containing "Unnamed: "
        df.rename(columns={"DOE            (Date of Enrollment MM/DD/YY)": "DOE (Date of Enrollment MM/DD/YY)"}, inplace=True)
        
        # Save the cleaned CSV (overwrite original)
        df.to_csv(file_path, index=False)
        print(f"Processed {filename}")