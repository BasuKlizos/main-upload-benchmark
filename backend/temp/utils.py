from pymongo import MongoClient
from datetime import datetime, timezone

client = MongoClient("mongodb://mongodb:27017")
db = client["task_db"]
task_collection = db["task_status"]

def create_task_record(task_id, job_type, metadata):
    task_collection.insert_one({
        "_id": task_id,
        "job_type": job_type,
        "status": "queued",
        "metadata": metadata,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    })

def update_task_status(task_id, status):
    task_collection.update_one(
        {"_id": task_id},
        {"$set": {"status": status, "updated_at": datetime.now(timezone.utc)}}
    )

def is_task_cancelled(task_id):
    task = task_collection.find_one({"_id": task_id})
    return task and task.get("status") == "cancelled"
