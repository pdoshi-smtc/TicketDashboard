import pandas as pd

file = "data/2025 SLA Report Spreadsheet.xlsx"
sheet = "1. Week AV and Other SA"

df = pd.read_excel(file, sheet_name=sheet, header=None)

data = []

weeks = None
dates = None

for i, row in df.iterrows():

    first = str(row[0]).strip()

    # stop processing when SERVICE AVAILABILITY % section starts
    if "SERVICE AVAILABILITY %" in first:
        break

    if "Week #" in first:
        weeks = list(row[1:])
        continue

    if "DOWNTIME" in first:
        dates = list(row[1:])
        continue

    if "SERVICE AVAILABILITY" in first:
        break

    if first in ["HOURS", "", "nan"]:
        continue

    service = first.replace("•","").strip()

    if dates is None:
        continue

    for col in range(len(dates)):

        downtime = row[col+1]

        if pd.notna(downtime):

            try:
                downtime = float(downtime)
            except:
                continue

            availability = ((168 - downtime) / 168) * 100

            data.append({
                "Service": service,
                "Week": int(weeks[col]),
                "Date": pd.to_datetime(dates[col]),
                "Availability": round(availability,2)
            })

result = pd.DataFrame(data)

result.to_csv("data/Week_AV_Other_SA.csv", index=False)

print("CSV created → Week_AV_Other_SA.csv")