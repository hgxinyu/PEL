import os
import pandas as pd


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
            

            df.rename(columns={"DOE            (Date of Enrollment MM/DD/YY)": "DOE (Date of Enrollment MM/DD/YY)"}, inplace=True)
            df.rename(columns={"DOE            (Date of Enrollment MM/DD/YY)": ""}, inplace=True)


            columns_to_drop = [col for col in df.columns if pd.isna(col) or 'Unnamed:' in str(col)]
            df.drop(columns=columns_to_drop, inplace=True)

            df = df.rename(columns={df.columns[10]: "PEL Wks. Level"})
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

folder_path = "PAS Freemont"
output_folder = "PAS Freemont CSV"

# turn_into_csv(folder_path, output_folder)
clean_csv_files(output_folder, nan_threshold=10)



