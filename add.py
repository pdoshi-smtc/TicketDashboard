import pandas as pd

file = "2025 SLA Report Spreadsheet.xlsx"
sheet = "2. Month SC SA"

df = pd.read_excel(file, sheet_name=sheet, header=None)

# ask user for data
week_number = int(input("Enter Week Number: "))
week_date = input("Enter Week Date (YYYY-MM-DD): ")

last_col = df.shape[1]

# make sure new column accepts mixed types
df[last_col] = None

for i, row in df.iterrows():

    first = str(row[0]).strip()

    if "HOURS" in first:
        df.loc[i, last_col] = 168

    elif "Week #" in first:
        df.loc[i, last_col] = week_number

    elif "DOWNTIME" in first:
        df.loc[i, last_col] = pd.to_datetime(week_date)

    else:
        if pd.notna(row[1]):
            val = input(f"Enter downtime for '{first}' (hours, default 0): ")
            df.loc[i, last_col] = float(val) if val else 0

# save file
with pd.ExcelWriter(file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df.to_excel(writer, sheet_name=sheet, index=False, header=False)

print("Week added successfully")