import os
from urllib.parse import quote_plus
from data_merger import merge_with_faker
from sqlalchemy import create_engine, text
import httplib2
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# --- PostgreSQL/Supabase Setup ---
POSTGRES_USER = os.getenv("SUPABASE_USER")
POSTGRES_PASSWORD = os.getenv("SUPABASE_PASSWORD")
POSTGRES_HOST = os.getenv("SUPABASE_HOST", "aws-1-ap-south-1.pooler.supabase.com")
POSTGRES_PORT = os.getenv("SUPABASE_PORT", "5432")
POSTGRES_DB = os.getenv("SUPABASE_DB", "postgres")

encoded_password = quote_plus(POSTGRES_PASSWORD)

engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}?sslmode=require"
)

# --- Google Sheets Setup ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH")
CREDS = Credentials.from_service_account_file(GOOGLE_CREDS_PATH, scopes=SCOPES)
unverified_http = httplib2.Http(disable_ssl_certificate_validation=True)
authorized_http = AuthorizedHttp(CREDS, http=unverified_http)
service = build("sheets", "v4", http=authorized_http)

SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Master Data")


def append_to_sheets_fresh_only(df):
    """
    Appends a dataframe to Google Sheets, including the header only if the sheet is empty.
    """
    if df.empty:
        print("✅ No fresh data to append to Google Sheets.")
        return

    # Check if the sheet is empty to decide whether to include the header
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1:A1"
    ).execute()

    sheet_is_empty = 'values' not in result

    if sheet_is_empty:
        # Append with header
        values_to_append = [df.columns.tolist()] + df.astype(str).values.tolist()
    else:
        # Append without header
        values_to_append = df.astype(str).values.tolist()

    body = {"values": values_to_append}

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:Z",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

    print(f"✅ Appended {len(df)} fresh rows to Google Sheets.")


if __name__ == "__main__":
    # Step 1: Generate fresh merged data
    df_final = merge_with_faker(fake_count=250)

    # Step 2: Fetch existing review_ids from DB
    try:
        with engine.connect() as conn:
            existing_ids = pd.read_sql("SELECT review_id FROM merged_data", conn)
        existing_ids_set = set(existing_ids["review_id"].tolist())
    except Exception as e:
        print("⚠️ Table does not exist yet, inserting everything.")
        existing_ids_set = set()

    # Step 3: Filter only fresh data
    fresh_df = df_final[~df_final["review_id"].isin(existing_ids_set)]

    if fresh_df.empty:
        print("✅ No new rows to insert.")
    else:
        # Step 4: Insert fresh rows into PostgreSQL
        try:
            fresh_df.to_sql("merged_data", engine, if_exists="append", index=False)
            print(f"✅ Inserted {len(fresh_df)} fresh rows into PostgreSQL.")
        except Exception as e:
            print(f"❌ PostgreSQL Error: {e}")

        # Step 5: Push updated data to Google Sheets
        try:
            # Use the new function to append only the fresh data
            append_to_sheets_fresh_only(fresh_df)
        except Exception as e:
            print(f"❌ Google Sheets Error: {e}")