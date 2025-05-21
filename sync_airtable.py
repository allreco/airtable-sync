import os
from ftplib import FTP
import pandas as pd
from pyairtable import Api
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Load environment variables
AIRTABLE_ACCESS_TOKEN = os.getenv("AIRTABLE_ACCESS_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

print("Access token loaded:", AIRTABLE_ACCESS_TOKEN)

FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FILENAME = f"231ALL/231ALL_{datetime.today().strftime('%Y%m%d')}.csv"

# Step 1: Connect to FTP and download the CSV
print("Downloading CSV from FTP...")
ftp = FTP(FTP_HOST)
ftp.login(FTP_USER, FTP_PASS)

with open("downloaded.csv", "wb") as f:
    ftp.retrbinary(f"RETR " + FTP_FILENAME, f.write)

ftp.quit()
print("CSV downloaded successfully.")

# Step 2: Load CSV into DataFrame
try:
    df = pd.read_csv("downloaded.csv", encoding="utf-8")
except UnicodeDecodeError:
    df = pd.read_csv("downloaded.csv", encoding="latin1")
# Replace NaN/NaT values with None for JSON compatibility
df = df.where(pd.notnull(df), None)
print(f"CSV loaded. {len(df)} rows found.")

# Step 3: Connect to Airtable
api = Api(AIRTABLE_ACCESS_TOKEN)
base = api.base(AIRTABLE_BASE_ID)
table = base.table(AIRTABLE_TABLE_NAME)

# Step 4: Delete existing records
def delete_batch(record_ids):
    try:
        table.batch_delete(record_ids)
    except Exception as e:
        print(f"Failed to delete batch {record_ids}: {e}")

# Fetch all record IDs
print("Fetching all record IDs...")
all_records = table.all()
record_ids = [record["id"] for record in all_records]
print(f"Found {len(record_ids)} records to delete.")

# Delete in batches of 10 using threads
BATCH_SIZE = 10
batches = [record_ids[i:i+BATCH_SIZE] for i in range(0, len(record_ids), BATCH_SIZE)]

print("Deleting records in parallel...")
with ThreadPoolExecutor(max_workers=15) as executor:
    executor.map(delete_batch, batches)

print("Deletion complete.")

# Step 5: Upload new records from CSV
print("Uploading new records to Airtable...")

records_to_create = df.to_dict(orient="records")
batch_size = 10

for i in range(0, len(records_to_create), batch_size):
    batch = records_to_create[i:i + batch_size]
    table.batch_create(batch)

print(f"Upload complete. {len(records_to_create)} records added to Airtable.")
