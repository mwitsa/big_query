from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json
import logging
import sys
import urllib.request
import pandas as pd
import os
from datetime import datetime, timedelta
import pytz
from drive_upload import build_drive_service, upload_to_drive

log = logging.getLogger(__name__)

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
    "410310": "โรงงานแปรรูปสุกรพระพุทธบาท",
    "594210": "โรงชำแหละสุกรขอนแก่น",
}


def build_client():
    log.info("[BigQuery] Loading credentials from GOOGLE_CREDENTIALS_JSON...")
    raw = os.environ["GOOGLE_CREDENTIALS_JSON"]
    info = json.loads(raw)
    creds = Credentials(
        token=None,
        refresh_token=info["refresh_token"],
        token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=info["client_id"],
        client_secret=info["client_secret"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    log.info("[BigQuery] Refreshing access token...")
    creds.refresh(Request())
    log.info("[BigQuery] Token refreshed successfully")
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    log.info(f"[BigQuery] Connected to project: {PROJECT_ID}")
    return client


def query_date(client, drive_service, date, plant_code):
    raw_date = date.strftime("%d%m%y")
    stk_doc_date = date.strftime("%Y-%m-%d")
    factory_name = FACTORIES.get(plant_code, plant_code)

    log.info(f"[{plant_code}] Starting query for {stk_doc_date} ({factory_name})")

    query = QUERY.format(plant_code=plant_code, stk_doc_date=stk_doc_date)
    log.info(f"[{plant_code}] Sending query to BigQuery...")
    query_job = client.query(query)
    df = query_job.to_dataframe()
    log.info(f"[{plant_code}] Query complete — {len(df)} rows returned")

    df.rename(columns={"DESC_LOC_GRP2": "DESC_LOC1", "DESC_LOC_GRP5": "DESC_LOC2"}, inplace=True)
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    month_folder = date.strftime("%Y-%m")
    output_dir = os.path.join("factory_code", plant_code, month_folder)
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{raw_date}.xlsx"
    output_file = os.path.join(output_dir, filename)

    log.info(f"[{plant_code}] Saving Excel file to {output_file}...")
    df.to_excel(output_file, index=False)
    log.info(f"[{plant_code}] Excel saved successfully ({os.path.getsize(output_file):,} bytes)")

    drive_folder_name = f"{plant_code} {factory_name}"
    log.info(f"[{plant_code}] Uploading to Google Drive ({drive_folder_name}/Data/{month_folder}/{filename})...")
    upload_to_drive(drive_service, output_file, drive_folder_name, month_folder, filename)
    log.info(f"[{plant_code}] Upload complete")


def run():
    bangkok = pytz.timezone("Asia/Bangkok")
    now = datetime.now(bangkok)
    today = now.date()
    target_date = datetime(today.year, today.month, today.day) - timedelta(days=1)

    log.info("=" * 60)
    log.info(f"Daily fetch triggered at {now.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")
    log.info(f"Target date: {target_date.strftime('%Y-%m-%d')} (yesterday)")
    log.info(f"Factories: {list(FACTORIES.keys())}")
    log.info("=" * 60)

    try:
        client = build_client()
    except Exception as e:
        log.error(f"[BigQuery] Failed to connect: {e}")
        raise

    try:
        log.info("[Drive] Connecting to Google Drive...")
        drive_service = build_drive_service()
        log.info("[Drive] Connected successfully")
    except Exception as e:
        log.error(f"[Drive] Failed to connect: {e}")
        raise

    success, failed = [], []
    for plant_code in FACTORIES:
        try:
            query_date(client, drive_service, target_date, plant_code)
            success.append(plant_code)
        except Exception as e:
            log.error(f"[{plant_code}] FAILED: {e}", exc_info=True)
            failed.append(plant_code)

    log.info("=" * 60)
    log.info(f"Run complete — Success: {success} | Failed: {failed}")
    log.info("=" * 60)

    if success:
        log.info("[YieldTracker] Triggering sync...")
        try:
            req = urllib.request.Request(
                "https://yieldtracker-production.up.railway.app/api/sync",
                method="POST",
                headers={"Content-Type": "application/json"},
                data=b"{}",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                log.info(f"[YieldTracker] Sync triggered — status {resp.status}")
        except Exception as e:
            log.error(f"[YieldTracker] Failed to trigger sync: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    run()
