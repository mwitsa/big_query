from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json
import pandas as pd
import os
from datetime import datetime, timedelta
import pytz
from drive_upload import build_drive_service, upload_to_drive

PROJECT_ID = "cpf-food-performance-tracking"
QUERY = """
    SELECT *
    FROM `cpf-food-performance-tracking.SWINE_KHONKAEN.VW_MOVEMENT_STOCK_SWINE`
    WHERE PLANT_CODE = '{plant_code}'
    AND STK_DOC_DATE = '{stk_doc_date}'
    ORDER BY CREATE_DATE ASC
"""

FACTORIES = {
    "262110": "โรงตัดแต่งสุกรขอนแก่น",
    "410310": "โรงงานพระบาท",
    "594210": "โรงชำแหละสุกรขอนแก่น",
}


def build_client():
    raw = os.environ["GOOGLE_CREDENTIALS_JSON"]
    info = json.loads(raw)
    creds = Credentials(
        token=None,
        refresh_token=info["refresh_token"],
        token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=info["client_id"],
        client_secret=info["client_secret"],
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
    )
    creds.refresh(Request())
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def query_date(client, drive_service, date, plant_code):
    raw_date = date.strftime("%d%m%y")
    stk_doc_date = date.strftime("%Y-%m-%d")
    factory_name = FACTORIES.get(plant_code, plant_code)

    print(f"\n--- {factory_name} ({plant_code}) | {stk_doc_date} ---")

    query = QUERY.format(plant_code=plant_code, stk_doc_date=stk_doc_date)
    df = client.query(query).to_dataframe()

    df.rename(columns={"DESC_LOC_GRP2": "DESC_LOC1", "DESC_LOC_GRP5": "DESC_LOC2"}, inplace=True)
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    print(f"  Rows: {len(df)}")

    month_folder = date.strftime("%Y-%m")
    output_dir = os.path.join("factory_code", plant_code, month_folder)
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{raw_date}.xlsx"
    output_file = os.path.join(output_dir, filename)
    df.to_excel(output_file, index=False)
    print(f"  Saved: {output_file}")

    upload_to_drive(drive_service, output_file, plant_code, month_folder, filename)


def run():
    bangkok = pytz.timezone("Asia/Bangkok")
    today = datetime.now(bangkok).date()
    # Fetch yesterday's data (finalized by 9 AM)
    target_date = datetime(today.year, today.month, today.day) - timedelta(days=1)

    print(f"=== Daily fetch started at {datetime.now(bangkok).strftime('%Y-%m-%d %H:%M:%S')} Bangkok ===")
    print(f"Target date: {target_date.strftime('%Y-%m-%d')}")

    client = build_client()
    print(f"Connected to BigQuery project: {PROJECT_ID}")

    drive_service = build_drive_service()
    print("Connected to Google Drive")

    for plant_code in FACTORIES:
        try:
            query_date(client, drive_service, target_date, plant_code)
        except Exception as e:
            print(f"  ERROR for {plant_code}: {e}")

    print("\n=== Done ===")


if __name__ == "__main__":
    run()
