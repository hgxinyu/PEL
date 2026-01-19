import pandas as pd

file1 = "Fremont progress.csv"
file2 = "Milpitas progress.csv"

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

df1['Center'] = "Fremont"
df2['Center'] = "Milpitas"


combined = pd.concat([df1, df2], ignore_index=True)

combined = combined.rename(columns={'Subject (M/E)': 'Subject'})
combined['Subject'] = combined['Subject'].replace({'E': 'English', 'M': 'Math'})

worksheets = pd.read_csv("worksheets.csv")
level_key = worksheets['PEL Wks. Level'].astype(str).str.strip()
level_to_lvs = pd.Series(worksheets['Lvs Value'].values, index=level_key)
combined_levels = combined['PEL Wks. Level'].astype(str).str.strip()
lvs_values = combined_levels.map(level_to_lvs)
if 'lvs' in combined.columns:
    combined = combined.drop(columns=['lvs'])
insert_at = combined.columns.get_loc('PEL Wks. Level') + 1
combined.insert(insert_at, 'lvs', pd.to_numeric(lvs_values, errors='coerce').astype('Int64'))

output_file = 'progress.csv'
combined.to_csv(output_file, index=False)

students_file1 = "Fremont students.csv"
students_file2 = "Milpitas students.csv"

students_df1 = pd.read_csv(students_file1)
students_df2 = pd.read_csv(students_file2)

students_df1['Center'] = "Fremont"
students_df2['Center'] = "Milpitas"

students_combined = pd.concat([students_df1, students_df2], ignore_index=True)

students_output_file = "students.csv"
students_combined.to_csv(students_output_file, index=False)

