import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
envStr = os.getenv()

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("DB_NAME")
collection_name = os.getenv("COLLECTION_NAME")

print(mongo_uri, db_name, collection_name)


db = client["log_processor"]          # creates DB if not exists
collection = db["incidents"]          # creates collection if not exists