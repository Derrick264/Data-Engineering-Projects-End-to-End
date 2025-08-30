import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import httplib2
from google_auth_httplib2 import AuthorizedHttp

from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak, KeepTogether
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# ------------------ 1) Load Environment & Setup Directories ------------------
load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH")
SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "SupabaseData")

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
CUSTOM_EMAIL_RECIPIENTS = os.getenv("CUSTOM_EMAIL_RECIPIENTS")
SKIP_EMAIL = os.getenv("SKIP_EMAIL", "false").lower() == "true"
REPORT_TYPE = os.getenv("REPORT_TYPE", "Full")

# Parse recipients - use custom recipients if provided, otherwise use default
if CUSTOM_EMAIL_RECIPIENTS:
    recipients_list = [email.strip() for email in CUSTOM_EMAIL_RECIPIENTS.split(',') if email.strip()]
elif EMAIL_RECEIVER:
    recipients_list = [EMAIL_RECEIVER]
else:
    recipients_list = []

# --- MODIFICATION: Define a directory for charts ---
CHARTS_DIR = "charts"
# --- MODIFICATION: Create the directory if it doesn't exist ---
os.makedirs(CHARTS_DIR, exist_ok=True)

# ------------------ 2) Fetch Google Sheets Data ------------------
print(f"📊 Accessing Google Sheets: {SPREADSHEET_ID}")

creds = Credentials.from_service_account_file(GOOGLE_CREDS_PATH, scopes=SCOPES)
http = AuthorizedHttp(creds, http=httplib2.Http(disable_ssl_certificate_validation=True))
service = build("sheets", "v4", http=http)

# Try SupabaseData first, fallback to Master Data
sheet_to_use = "SupabaseData"
rows = None

try:
    print(f"📋 Attempting to use sheet: {sheet_to_use}")
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_to_use}!A:Z"
    ).execute()

    rows = result.get("values", [])
    if not rows:
        raise ValueError("Sheet is empty")

    print(f"✅ Successfully loaded {len(rows)} rows from {sheet_to_use} sheet")

except Exception as e:
    print(f"❌ Error accessing {sheet_to_use} sheet: {str(e)}")
    print("🔄 Attempting to use fallback sheet: Master Data")

    try:
        sheet_to_use = "Master Data"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_to_use}!A:Z"
        ).execute()

        rows = result.get("values", [])
        if not rows:
            raise ValueError("Master Data sheet is also empty")

        print(f"✅ Successfully loaded {len(rows)} rows from {sheet_to_use} sheet (fallback)")

    except Exception as fallback_error:
        print(f"❌ Error accessing Master Data sheet: {str(fallback_error)}")
        print("🔍 Available sheets in the spreadsheet:")
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
            sheets = spreadsheet.get('sheets', [])
            for sheet in sheets:
                sheet_name = sheet.get("properties", {}).get("title", "Unknown")
                print(f"   - {sheet_name}")
            print("💡 Make sure either 'SupabaseData' or 'Master Data' sheet exists and contains data")
        except:
            print("   Could not retrieve sheet list")
        raise Exception("Neither SupabaseData nor Master Data sheets are accessible")

# ------------------ 3) DataFrame ------------------
max_len = max(len(rows[0]), max(len(r) for r in rows[1:]))
for r in rows:
    while len(r) < max_len:
        r.append("")
while len(rows[0]) < max_len:
    rows[0].append(f"extra_col_{len(rows[0]) + 1}")

df = pd.DataFrame(rows[1:], columns=rows[0])
df.columns = df.columns.str.strip().str.lower()

# Cast numeric
for col in ["performance_rating", "engagement_score", "age", "tenure_years"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df["status"] = df["status"].astype(str).str.strip().str.lower()

# ------------------ 4) KPIs ------------------
total_employees = len(df)
active_employees = df["status"].eq("active").sum()
exited_employees = df["status"].eq("exited").sum()

avg_perf = df["performance_rating"].mean().round(2)
avg_eng = df["engagement_score"].mean().round(2)
avg_age = df["age"].mean().round(1)
avg_tenure = df.loc[df["status"].eq("exited"), "tenure_years"].mean().round(1)
attrition_rate = round((exited_employees / total_employees) * 100, 1)

# ------------------ 5) Department Summary ------------------
count_col = "name" if "name" in df.columns else df.columns[0]

dept_summary = (
    df.groupby("department")
    .agg(
        headcount=(count_col, "count"),
        avg_performance=("performance_rating", "mean"),
        avg_engagement=("engagement_score", "mean"),
        exited=("status", lambda x: x.eq("exited").sum()),
        avg_tenure=("tenure_years", lambda x: x[df.loc[x.index, "status"].eq("exited")].mean())
    )
    .reset_index()
)

dept_summary["attrition_pct"] = ((dept_summary["exited"] / dept_summary["headcount"]) * 100).round(1)
dept_summary["avg_performance"] = dept_summary["avg_performance"].round(2)
dept_summary["avg_engagement"] = dept_summary["avg_engagement"].round(2)
dept_summary["avg_tenure"] = dept_summary["avg_tenure"].round(1)

# ------------------ 6) Charts ------------------
# Department Headcount
dept_counts = df.groupby(["department", "status"]).size().unstack(fill_value=0)
dept_counts.plot(kind="bar", stacked=True, figsize=(8, 5))
plt.title("Employee Count by Department (Active vs Exited)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
# --- MODIFICATION: Save chart to the defined directory ---
plt.savefig(os.path.join(CHARTS_DIR, "dept_headcount.png"))
plt.close()

# Performance & Engagement
plt.figure(figsize=(8, 5))
x = range(len(dept_summary))
plt.bar([i - 0.2 for i in x], dept_summary["avg_performance"], width=0.4, label="Performance")
plt.bar([i + 0.2 for i in x], dept_summary["avg_engagement"], width=0.4, label="Engagement")
plt.xticks(x, dept_summary["department"], rotation=45, ha="right")
plt.title("Average Performance & Engagement by Department")
plt.legend()
plt.tight_layout()
# --- MODIFICATION: Save chart to the defined directory ---
plt.savefig(os.path.join(CHARTS_DIR, "dept_perf.png"))
plt.close()

# Salary Band
if "salary_band" in df.columns:
    df["salary_band"].value_counts().plot(kind="pie", autopct="%1.1f%%", figsize=(6, 6))
    plt.title("Employee Distribution by Salary Band")
    plt.ylabel("")
    plt.tight_layout()
    # --- MODIFICATION: Save chart to the defined directory ---
    plt.savefig(os.path.join(CHARTS_DIR, "salary_band.png"))
    plt.close()

# Gender
if "gender" in df.columns:
    df["gender"].value_counts().plot(kind="pie", autopct="%1.1f%%", figsize=(6, 6))
    plt.title("Gender Distribution")
    plt.ylabel("")
    plt.tight_layout()
    # --- MODIFICATION: Save chart to the defined directory ---
    plt.savefig(os.path.join(CHARTS_DIR, "gender_dist.png"))
    plt.close()

# Performance Dist
bins = [1, 2, 3, 4, 5]
labels = ["1–2", "2–3", "3–4", "4–5"]
df["perf_bucket"] = pd.cut(df["performance_rating"], bins=bins, labels=labels, include_lowest=True)
df["perf_bucket"].value_counts().sort_index().plot(kind="bar", figsize=(7, 5))
plt.title("Performance Rating Distribution")
plt.tight_layout()
# --- MODIFICATION: Save chart to the defined directory ---
plt.savefig(os.path.join(CHARTS_DIR, "perf_dist.png"))
plt.close()


# ------------------ 7) Trends Fix (No Overlapping Labels) ------------------
def plot_trend(dt_series, title, filename, color="blue"):
    s = pd.to_datetime(dt_series, errors="coerce").dropna().dt.to_period("M").value_counts().sort_index()
    if s.empty:
        return
    idx = s.index.to_timestamp()
    labels = [d.strftime("%b %Y") for d in idx]

    plt.figure(figsize=(12, 5))
    plt.plot(range(len(s)), s.values, marker="o", color=color)
    plt.title(title)
    plt.xlabel("Month")
    plt.ylabel("Count")
    plt.xticks(range(len(labels)), labels, rotation=45, ha="right")

    ax = plt.gca()
    ax.xaxis.set_major_locator(ticker.MultipleLocator(3))

    plt.tight_layout()
    # --- MODIFICATION: Save chart to the defined directory ---
    plt.savefig(os.path.join(CHARTS_DIR, filename))
    plt.close()


if "joining_date" in df.columns:
    plot_trend(df["joining_date"], "Monthly Hirings", "monthly_hirings.png", color="blue")

if "exit_date" in df.columns:
    plot_trend(df["exit_date"], "Monthly Exits", "monthly_exits.png", color="red")

# ------------------ 8) PDF Report ------------------
doc = SimpleDocTemplate("HR_Analytics_Report.pdf", pagesize=A4,
                        leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.5 * cm, bottomMargin=1.5 * cm)
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="Small", parent=styles["Normal"], fontSize=9))

story = []

# Determine report content based on type
is_summary = REPORT_TYPE == "Summary"

# 1. Executive Summary
title = "HR Analytics Report — Summary" if is_summary else "HR Analytics Report — Comprehensive"
story.append(Paragraph(title, styles["Title"]))
story.append(Spacer(1, 12))

exec_data = [
    ["Total Employees", total_employees],
    ["Active Employees", active_employees],
    ["Exited Employees", exited_employees],
    ["Attrition Percentage", f"{attrition_rate}%"],
    ["Average Tenure (Exited)", f"{avg_tenure} years"],
    ["Average Performance", avg_perf],
    ["Average Engagement", avg_eng],
    ["Average Age", avg_age]
]
exec_table = Table(exec_data, colWidths=[7 * cm, 7 * cm])
exec_table.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold")]))
story.append(exec_table)
story.append(Spacer(1, 20))

# 2. Department Summary Table
story.append(Paragraph("Department Summary", styles["Heading2"]))
dept_table_data = [
    ["Department", "Headcount", "Attrition %", "Avg Performance", "Avg Engagement", "Avg Tenure (Exited)"]]
for _, r in dept_summary.iterrows():
    dept_table_data.append([
        Paragraph(str(r["department"]), styles["Small"]),
        int(r["headcount"]),
        f"{r['attrition_pct']}%",
        f"{r['avg_performance']:.2f}" if pd.notnull(r["avg_performance"]) else "—",
        f"{r['avg_engagement']:.2f}" if pd.notnull(r["avg_engagement"]) else "—",
        f"{r['avg_tenure']:.1f}" if pd.notnull(r["avg_tenure"]) else "—"
    ])
dept_table = Table(dept_table_data, colWidths=[4 * cm, 2 * cm, 2.5 * cm, 3 * cm, 3 * cm, 3 * cm], repeatRows=1)
dept_table.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#333333")),
                                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                                ("ALIGN", (1, 1), (-1, -1), "CENTER")]))
story.append(KeepTogether(dept_table))

if not is_summary:
    story.append(PageBreak())

# --- MODIFICATION: Construct full path for all charts below ---
dept_headcount_path = os.path.join(CHARTS_DIR, "dept_headcount.png")
dept_perf_path = os.path.join(CHARTS_DIR, "dept_perf.png")
perf_dist_path = os.path.join(CHARTS_DIR, "perf_dist.png")
monthly_hirings_path = os.path.join(CHARTS_DIR, "monthly_hirings.png")
monthly_exits_path = os.path.join(CHARTS_DIR, "monthly_exits.png")
gender_dist_path = os.path.join(CHARTS_DIR, "gender_dist.png")
salary_band_path = os.path.join(CHARTS_DIR, "salary_band.png")

if is_summary:
    # Summary Report: Only key charts
    story.append(Spacer(1, 20))
    story.append(Paragraph("Key Metrics Overview", styles["Heading2"]))

    # Add only essential charts for summary
    if os.path.exists(dept_headcount_path):
        story.append(Image(dept_headcount_path, width=14 * cm, height=7 * cm))
        story.append(Spacer(1, 12))

    if os.path.exists(dept_perf_path):
        story.append(Image(dept_perf_path, width=14 * cm, height=7 * cm))

else:
    # Full Report: All sections with charts
    # 3. Workforce Metrics Graphs
    story.append(Paragraph("Workforce Metrics", styles["Heading2"]))
    if os.path.exists(dept_headcount_path):
        story.append(Image(dept_headcount_path, width=16 * cm, height=9 * cm))
    story.append(PageBreak())

    # 4. Performance & Engagement Graphs
    story.append(Paragraph("Performance & Engagement", styles["Heading2"]))
    if os.path.exists(dept_perf_path):
        story.append(Image(dept_perf_path, width=16 * cm, height=9 * cm))
    if os.path.exists(perf_dist_path):
        story.append(Image(perf_dist_path, width=16 * cm, height=9 * cm))
    story.append(PageBreak())

    # 5. Trends
    story.append(Paragraph("Trends", styles["Heading2"]))
    if os.path.exists(monthly_hirings_path):
        story.append(Image(monthly_hirings_path, width=16 * cm, height=8 * cm))
    if os.path.exists(monthly_exits_path):
        story.append(Image(monthly_exits_path, width=16 * cm, height=8 * cm))
    story.append(PageBreak())

    # 6. Demographics & Diversity
    story.append(Paragraph("Demographics & Diversity", styles["Heading2"]))
    story.append(Spacer(1, 12))

    charts_on_one_page = []

    if os.path.exists(gender_dist_path):
        img1 = Image(gender_dist_path)
        img1.drawHeight = img1.imageHeight * cm / img1.imageWidth * 10
        img1.drawWidth = 10 * cm
        charts_on_one_page.append(img1)
        charts_on_one_page.append(Spacer(1, 0.5 * cm))

    if os.path.exists(salary_band_path):
        img2 = Image(salary_band_path)
        img2.drawHeight = img2.imageHeight * cm / img2.imageWidth * 10
        img2.drawWidth = 10 * cm
        charts_on_one_page.append(img2)

    if charts_on_one_page:
        story.append(KeepTogether(charts_on_one_page))

doc.build(story)
print(f"✅ PDF {REPORT_TYPE} Report Generated")

# ------------------ 9) Email ------------------
if not SKIP_EMAIL and recipients_list:
    subject = f"HR Analytics {REPORT_TYPE} Report"
    report_description = "Summary Report with key metrics" if REPORT_TYPE == "Summary" else "Comprehensive Report with detailed analysis"
    body = f"Hello,\n\nPlease find attached the HR Analytics {REPORT_TYPE} Report.\n\n{report_description} is included in this delivery.\n\nBest,\nHR Analytics Bot"

    # Create server connection once
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)

        # Send to each recipient
        for recipient in recipients_list:
            msg = MIMEMultipart()
            msg["From"] = EMAIL_SENDER
            msg["To"] = recipient
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            with open("HR_Analytics_Report.pdf", "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment; filename=HR_Analytics_Report.pdf")
            msg.attach(part)

            server.send_message(msg)
            print(f"✅ Email Sent to {recipient}")

    print(f"✅ Report successfully sent to {len(recipients_list)} recipient(s): {', '.join(recipients_list)}")
elif not SKIP_EMAIL and not recipients_list:
    print("⚠️ No email recipients configured")
else:
    print("✅ Email sending skipped (SKIP_EMAIL=true)")
