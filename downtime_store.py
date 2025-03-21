import time
import datetime
import pandas as pd
from pymongo import MongoClient                                 # type: ignore
from downtime import DowntimeCalculator

MONGO_URL = "mongodb://localhost:27017/"
DB_NAME = "machine_data"
COLLECTION_NAME = "data_downtime"
MACHINE_IDS = ["FEGOR", "HITACHI_600T", "HMT_201T"]

calculator = DowntimeCalculator()

def get_last_entry(mongo_url, db_name, collection_name, machine_id):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    
    last_entry = collection.find_one({"machine_id": machine_id}, sort=[("_id", -1)])
    
    client.close()
    return last_entry

def merge_or_store_downtime(mongo_url, db_name, collection_name, machine_id, downtime_data, start_time_ist, end_time_ist):
    if not downtime_data.get("downtime_periods"):  
        return  

    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    
    last_entry = get_last_entry(mongo_url, db_name, collection_name, machine_id)
    merged = False
    unmerged_periods = [] 

    if last_entry:
        last_downtime = last_entry.get("downtime_data", {})

        if last_downtime.get("downtime_periods"):
            last_periods = last_downtime["downtime_periods"]
            last_end_time = last_periods[-1]["end_time"]
            last_end_dt = datetime.datetime.strptime(last_end_time, "%Y-%m-%dT%H:%M:%S.%fZ")

            for period in downtime_data["downtime_periods"]:
                new_start_dt = datetime.datetime.strptime(period["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
                time_diff = (new_start_dt - last_end_dt).total_seconds()

                if time_diff <= 30:
                    last_periods[-1]["end_time"] = period["end_time"]
                    last_periods[-1]["duration_seconds"] += period["duration_seconds"]
                    last_downtime["total_downtime_duration"] += period["duration_seconds"]
                    merged = True
                else:
                    unmerged_periods.append(period)

            if merged:
                collection.update_one(
                    {"_id": last_entry["_id"]},
                    {"$set": {"end_time": end_time_ist, "downtime_data": last_downtime}}
                )
                print(f"Merged downtime for {machine_id}.")

    else:
        unmerged_periods = downtime_data["downtime_periods"]

    if unmerged_periods:
        data = {
            "timestamp": datetime.datetime.now().isoformat() + 'Z',
            "start_time": start_time_ist,
            "end_time": end_time_ist,
            "machine_id": machine_id,
            "downtime_data": {
                "downtime_periods": unmerged_periods,
                "total_downtime_duration": sum(p["duration_seconds"] for p in unmerged_periods)
            }
        }
        collection.insert_one(data)
        print(f"Stored all unmerged downtime periods for {machine_id} in a single document.")

    client.close()

def main():
    while True:
        now_ist = datetime.datetime.now().replace(second=0, microsecond=0)
        start_time_utc = (now_ist - datetime.timedelta(minutes=30)).isoformat() + 'Z'
        end_time_utc = now_ist.isoformat() + 'Z'

        print(f"Checking downtime for slot: {start_time_utc} to {end_time_utc} IST...")

        for machine_id in MACHINE_IDS:
            downtime_data = calculator.calculate_downtime_and_connection_lost(start_time_utc, end_time_utc, machine_id)
            
            merge_or_store_downtime(MONGO_URL, DB_NAME, COLLECTION_NAME, machine_id, downtime_data, start_time_utc, end_time_utc)

        print("Sleeping for 30 minutes...")
        time.sleep(1800)

if __name__ == "__main__":
    main()
