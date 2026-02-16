from jira import JIRA
from collections import defaultdict
from dateutil import parser
from datetime import datetime, timezone
import pandas as pd
import os

# ==========================
# CONFIG
# ==========================
JIRA_URL = os.getenv("JIRA_BASE_URL", "https://sierrawireless.atlassian.net")
USERNAME = os.getenv("JIRA_USER_EMAIL")
PASSWORD = os.getenv("JIRA_API_TOKEN")

JQL = 'project = GNOC AND issuetype = Incident AND created >= "2026-01-17" '

VALID_STATUSES = {
    "OPEN",
    "WORK IN PROGRESS",
    "IN REVIEW",
    "COMPLETED",
    "CANCELLED",
    "CANCELED",
    "CLOSED"
}

FINAL_STATUSES = {
    "COMPLETED",
    "CLOSED",
    "CANCELLED",
    "CANCELED"
}

# ==========================
# SLA CONFIG (MINUTES)
# ==========================
SLA_MINUTES = {
    "HIGHEST": 2 * 60,
    "HIGH": 4 * 60,
    "MEDIUM": 24 * 60,
    "LOW": 48 * 60,
    "LOWEST": 60 * 60
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

print("Fetching Jira issues...")

issues = jira.enhanced_search_issues(
    jql_str=JQL,
    expand="changelog",
    maxResults=False
)

print(f"Total tickets fetched: {len(issues)}")

rows = []
now = datetime.now(timezone.utc)

# ==========================
# PROCESS ISSUES
# ==========================
for idx, issue in enumerate(issues, start=1):
    fields = issue.fields

    if idx % 100 == 0:
        print(f"Processed {idx} tickets...")

    created_time = parser.parse(fields.created)
    resolution_time = parser.parse(fields.resolutiondate) if fields.resolutiondate else now

    status_changes = []
    for history in issue.changelog.histories:
        for item in history.items:
            if item.field == "status":
                status_changes.append({
                    "from": item.fromString.upper() if item.fromString else None,
                    "to": item.toString.upper(),
                    "time": parser.parse(history.created)
                })

    status_changes.sort(key=lambda x: x["time"])

    timeline = []
    if status_changes and status_changes[0]["from"]:
        initial_status = status_changes[0]["from"]
    else:
        initial_status = fields.status.name.upper()

    if initial_status in VALID_STATUSES:
        timeline.append((initial_status, created_time))

    for change in status_changes:
        if change["to"] in VALID_STATUSES:
            timeline.append((change["to"], change["time"]))

    timeline.sort(key=lambda x: x[1])

    time_in_status = defaultdict(float)

    for i in range(len(timeline)):
        status, start = timeline[i]
        end = timeline[i + 1][1] if i + 1 < len(timeline) else resolution_time

        if status in FINAL_STATUSES:
            continue

        if end > start:
            time_in_status[status] += (end - start).total_seconds()

    open_min = int(time_in_status.get("OPEN", 0) / 60)
    wip_min = int(time_in_status.get("WORK IN PROGRESS", 0) / 60)
    review_min = int(time_in_status.get("IN REVIEW", 0) / 60)
    completed_min = int(time_in_status.get("COMPLETED", 0) / 60)
    cancelled_min = int((time_in_status.get("CANCELLED", 0) + time_in_status.get("CANCELED", 0)) / 60)
    closed_min = int(time_in_status.get("CLOSED", 0) / 60)

    time_to_resolution_min = open_min + wip_min + review_min

    sla_minutes = get_sla_minutes(fields.priority.name if fields.priority else None)

    if sla_minutes is None:
        sla_status = None
        time_breached_min = 0
    else:
        if time_to_resolution_min > sla_minutes:
            sla_status = "Breached"
            time_breached_min = time_to_resolution_min - sla_minutes
        else:
            sla_status = "Met"
            time_breached_min = 0

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

        "OPEN (Minutes)": open_min,
        "WORK IN PROGRESS (Minutes)": wip_min,
        "IN REVIEW (Minutes)": review_min,
        "COMPLETED (Minutes)": completed_min,
        "CANCELLED (Minutes)": cancelled_min,
        "CLOSED (Minutes)": closed_min,

        "Time to Resolution (Minutes)": time_to_resolution_min,
        "SLA Status": sla_status,
        "Time Breached (Minutes)": time_breached_min
    })

# ==========================
# EXPORT CSV
# ==========================
os.makedirs("data", exist_ok=True)
filename = "data/GNOC_Incident_Time.csv"

df = pd.DataFrame(rows)
df.to_csv(filename, index=False)

print(f"Exported {len(df)} tickets → {filename}")
