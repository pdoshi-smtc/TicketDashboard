import pandas as pd

file = "2025 SLA Report Spreadsheet.xlsx"
sheet = "1. Week SC SA"

df = pd.read_excel(file, sheet_name=sheet, header=None)

data = []

weeks = None
dates = None
current_offering = None

for i, row in df.iterrows():

    first = str(row[0]).strip()

    # capture week numbers
    if "Week #" in first:
        weeks = list(row[1:])
        continue

    # capture date row
    if "DOWNTIME" in first:
        dates = list(row[1:])
        continue

    # detect offering rows
    if first.startswith("ADVANCED") or first.startswith("ESSENTIALS"):
        current_offering = first
        continue

    # detect service rows
    if "•" in first:

        service = first.replace("•","").strip()

        for col in range(len(dates)):

            downtime = row[col+1]

            if pd.notna(downtime):

                try:
                    downtime = float(downtime)
                except:
                    continue

                availability = ((168 - downtime) / 168) * 100

                data.append({
                    "Offering": current_offering,
                    "Service": service,
                    "Week": int(weeks[col]),
                    "Date": pd.to_datetime(dates[col]),
                    "Availability": round(availability,2)
                })

result = pd.DataFrame(data)

result.to_csv("data/Week_SC_SA.csv", index=False)

print("CSV created → data/Week_SC_SA.csv")