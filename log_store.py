import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("DB_NAME")
collection_name = os.getenv("COLLECTION_NAME")

client = MongoClient(mongo_uri)
db = client[db_name]          # creates DB if not exists
collection = db[collection_name]          # creates collection if not exists

try:
    client.admin.command('ping')
    print("Connected to MongDB Atlas successfully!")
except Exception as e:
    print(f"Connection failed: {e}")

df = pd.read_csv("data/incident_event_log.csv")
print(df.shape)
print(df.columns)

### Data Cleaning
df = df.replace("?", None)

# Convert date columns to datetime
date_columns = ['opened_at', 'sys_created_at', 'sys_updated_at', 'resolved_at', 'closed_at' ]
for col in date_columns:
    df[col] = df[col].astype(str).replace('NaT', None)

df = df.where(pd.notnull(df), None)

#Convert dataframe to list of dictionaries
records = df.to_dict(orient="records")

batch_size = 1000

if collection.count_documents({}) > 0:
    print(f"Data already exists! {collection.counts_documents({})} records found, skipping insert")
else:
    for i in range(0, len(records), batch_size):
        try:
            batch = records[i:i + batch_size]
            collection.insert_many(batch)
            print(f"Inserted records {i} to {i + len(batch)}")
        except Exception as e:
            print(f"Failed at batch {i}: {e}")

print(f"Total records in DB: {collection.count_documents({})}")