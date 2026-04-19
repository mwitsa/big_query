from google.cloud import bigquery
import pandas as pd
import os

# Config
PROJECT_ID = "cpf-food-performance-tracking"
QUERY = """
    SELECT *
    FROM `cpf-food-performance-tracking.SWINE_KHONKAEN.VW_MOVEMENT_STOCK_SWINE`
    WHERE PLANT_CODE = '{plant_code}'
    AND STK_DOC_DATE = '{stk_doc_date}'
    ORDER BY CREATE_DATE ASC
"""

def parse_dates(user_input):
    """Parse single date or date range (e.g. '120326' or '120326 - 190326')."""
    from datetime import datetime, timedelta
    if "-" in user_input:
        parts = [p.strip() for p in user_input.split("-", 1)]
        start = datetime.strptime(parts[0], "%d%m%y")
        end = datetime.strptime(parts[1], "%d%m%y")
        dates = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)
        return dates
    else:
        return [datetime.strptime(user_input.strip(), "%d%m%y")]

def query_date(client, date, plant_code):
    """Query BigQuery for a single date and save to Excel."""
    raw_date = date.strftime("%d%m%y")
    stk_doc_date = date.strftime("%Y-%m-%d")

    print(f"\n--- Querying date: {stk_doc_date} ({raw_date}) ---")

    query = QUERY.format(plant_code=plant_code, stk_doc_date=stk_doc_date)
    query_job = client.query(query)
    df = query_job.to_dataframe()

    # Rename columns
    df.rename(columns={"DESC_LOC_GRP2": "DESC_LOC1", "DESC_LOC_GRP5": "DESC_LOC2"}, inplace=True)

    # Strip timezone info from datetime columns so Excel can handle them
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    print(f"Total rows: {len(df)}")

    month_folder = date.strftime("%Y-%m")
    output_dir = os.path.join("factory_code", plant_code, month_folder)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{raw_date}.xlsx")
    df.to_excel(output_file, index=False)
    print(f"Saved to {output_file}")

FACTORIES = {
    "1": ("262110", "โรงตัดแต่งสุกรขอนแก่น"),
    "2": ("410310", "โรงงานพระบาท"),
    "3": ("594210", "โรงชำแหละสุกรขอนแก่น"),
}

def choose_factory():
    print("Select factory:")
    for key, (code, name) in FACTORIES.items():
        print(f"  {key}. {code} {name}")
    print(f"  4. All factories")
    while True:
        choice = input("Enter choice (1-4): ").strip()
        if choice in FACTORIES:
            code, name = FACTORIES[choice]
            print(f"Selected: {code} {name}")
            return [code]
        if choice == "4":
            print("Selected: All factories")
            return [code for code, _ in FACTORIES.values()]
        print("Invalid choice. Please enter 1, 2, 3, or 4.")

def main():
    plant_codes = choose_factory()
    user_input = input("Enter date (DDMMYY) or range (DDMMYY - DDMMYY): ").strip()
    dates = parse_dates(user_input)

    client = bigquery.Client(project=PROJECT_ID)
    print(f"Connected to project: {PROJECT_ID}")
    print(f"Processing {len(dates)} date(s) x {len(plant_codes)} factory(s)...")

    for plant_code in plant_codes:
        for date in dates:
            query_date(client, date, plant_code)

if __name__ == "__main__":
    main()
