from jira import JIRA
from collections import defaultdict
from dateutil import parser
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

# ==========================
# CONFIG
# ==========================
JIRA_URL = os.getenv("JIRA_BASE_URL", "https://sierrawireless.atlassian.net")
USERNAME = os.getenv("JIRA_USER_EMAIL")
PASSWORD = os.getenv("JIRA_API_TOKEN")

JQL = "project = GNOC AND issuetype = Incident"

VALID_STATUSES = {
    "OPEN",
    "WORK IN PROGRESS",
    "IN REVIEW",
    "COMPLETED",
    "CANCELLED",
    "CLOSED"
}

# ==========================
# SLA CONFIG (MINUTES)
# ==========================
SLA_MINUTES = {
    "HIGHEST": 2 * 60,   # P1
    "HIGH": 4 * 60,      # P2
    "MEDIUM": 24 * 60,   # P3
    "LOW": 48 * 60,      # P4
    "LOWEST": 60 * 60    # P5
}

def get_sla_minutes(priority_name):
    if not priority_name:
        return None

    priority_name = priority_name.upper()

    for key, minutes in SLA_MINUTES.items():
        if key in priority_name:
            return minutes

    return None

# ==========================
# CONNECT TO JIRA
# ==========================
jira = JIRA(
    server=JIRA_URL,
    basic_auth=(USERNAME, PASSWORD)
)

# ==========================
# FETCH ALL ISSUES (JIRA CLOUD SAFE)
# ==========================
print("Starting Jira issue fetch (ALL issues)...")

issues = jira.enhanced_search_issues(
    jql_str=JQL,
    expand="changelog",
    maxResults=20
)

print(f"Total tickets fetched: {len(issues)}")

# ==========================
# PROCESS ISSUES
# ==========================
rows = []
now = datetime.now(timezone.utc)
count = 0

for issue in issues:
    count += 1
    fields = issue.fields

    if count % 100 == 0:
        print(f"Processed {count} tickets...")

    # ---- Build status timeline ----
    timeline = []
    created_time = parser.parse(fields.created)
    timeline.append((fields.status.name.upper(), created_time))

    for history in issue.changelog.histories:
        for item in history.items:
            if item.field == "status":
                status = item.toString.upper()
                if status in VALID_STATUSES:
                    timeline.append((status, parser.parse(history.created)))

    timeline.sort(key=lambda x: x[1])

    # ---- Time per status (SECONDS) ----
    time_in_status = defaultdict(float)

    for i in range(len(timeline)):
        status, start = timeline[i]
        end = timeline[i + 1][1] if i + 1 < len(timeline) else now
        time_in_status[status] += (end - start).total_seconds()

    # ---- Convert to MINUTES (NUMERIC) ----
    open_min = int(time_in_status.get("OPEN", 0) / 60)
    wip_min = int(time_in_status.get("WORK IN PROGRESS", 0) / 60)
    review_min = int(time_in_status.get("IN REVIEW", 0) / 60)
    completed_min = int(time_in_status.get("COMPLETED", 0) / 60)
    cancelled_min = int(time_in_status.get("CANCELLED", 0) / 60)
    closed_min = int(time_in_status.get("CLOSED", 0) / 60)

    # ---- Time to Resolution (OPEN + WIP + REVIEW) ----
    time_to_resolution_min = (open_min + wip_min + review_min)

    # ---- SLA Status ----
    sla_minutes = get_sla_minutes(fields.priority.name if fields.priority else None)

    if sla_minutes is None:
        sla_status = None
    else:
        if fields.resolutiondate:
            sla_status = "Met" if time_to_resolution_min <= sla_minutes else "Breached"
        else:
            sla_status = "Within SLA" if time_to_resolution_min <= sla_minutes else "Breached"

    # ---- CSV Row ----
    rows.append({
        "Issue key": issue.key,
        "Summary": fields.summary,
        "Issue Type": fields.issuetype.name if fields.issuetype else None,
        "Status": fields.status.name if fields.status else None,
        "Project name": fields.project.name if fields.project else None,
        "Project type": fields.project.projectTypeKey if fields.project else None,
        "Priority": fields.priority.name if fields.priority else None,
        "Resolution": fields.resolution.name if fields.resolution else None,
        "Assignee": fields.assignee.displayName if fields.assignee else "Unassigned",
        "Reporter": fields.reporter.displayName if fields.reporter else None,
        "Creator": fields.creator.displayName if fields.creator else None,
        "Created": fields.created,
        "Updated": fields.updated,
        "Resolved": fields.resolutiondate,
        "Components": ", ".join(c.name for c in fields.components) if fields.components else None,
        "Source / Detection": fields.customfield_10040,
        "Investigation Type": fields.customfield_10563,

        # ---- TIME AS NUMBERS (MINUTES) ----
        "OPEN (Minutes)": open_min,
        "WORK IN PROGRESS (Minutes)": wip_min,
        "IN REVIEW (Minutes)": review_min,
        "COMPLETED (Minutes)": completed_min,
        "CANCELLED (Minutes)": cancelled_min,
        "CLOSED (Minutes)": closed_min,
        "Time to Resolution (Minutes)": time_to_resolution_min,

        # ---- SLA ----
        "SLA Status": sla_status,
    })

# ==========================
# EXPORT CSV File
# ==========================
filename = "data/GNOC_Incident_Time.csv"
df = pd.DataFrame(rows)
df.to_csv(filename, index=False)

print(f"Exported {len(df)} tickets to {filename}")