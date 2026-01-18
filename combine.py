import pandas as pd

file1 = "Freemont progress.csv"
file2 = "Milpitas progress.csv"

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

df1['location'] = "Freemont"
df2['location'] = "Milpitas"


combined = pd.concat([df1, df2], ignore_index=True)


output_file = 'combined progress.csv'
combined.to_csv(output_file, index=False)

