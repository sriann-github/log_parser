import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("DB_NAME")
collection_name = os.getenv("COLLECTION_NAME")

client = MongoClient(mongo_uri)
db = client[db_name] # creates DB if not exists
collection = db[collection_name] # creates collection if not exists

try:
    client.admin.command('ping')
    print("Connected to MongoDB Atlas successfully!")
except Exception as e:
    print(f"Connection failed: {e}")

def count_by_priority(collection):
    pipeline = [
        {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        print(f"Priority: {result['_id']} | Count: {result['count']}")

   # [{"$group":{"_id" : "$priority", "count": {"$sum": 1}, "avg_reopens": {"$avg": "$reopen_count"}, "max_reassignments": {"$max": "$reassignment_count"}}}]

count_by_priority(collection)

def count_by_state(collection):
    pipeline = [
        {"$group": {"_id": "$incident_state", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        print(f"State: {result['_id']} | Count: {result['count']}")

count_by_state(collection)