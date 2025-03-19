import time
import datetime
import pandas as pd
from pymongo import MongoClient  # type: ignore
from downtime import DowntimeCalculator

MONGO_URL = "mongodb://localhost:27017/"
DB_NAME = "machine_data"
COLLECTION_NAME = "data_downtime"
MACHINE_IDS = ["FEGOR", "HITACHI_600T", "HMT_201T"]

calculator = DowntimeCalculator()

def get_last_entry(mongo_url, db_name, collection_name):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    last_entry = collection.find_one(sort=[("_id", -1)])
    client.close()
    return last_entry

def merge_or_store_downtime(mongo_url, db_name, collection_name, downtime_data, start_time_ist, end_time_ist):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    
    last_entry = get_last_entry(mongo_url, db_name, collection_name)
    
    if last_entry:
        last_downtime_data = last_entry.get("downtime_data", {})
        df_last = pd.DataFrame.from_dict(last_downtime_data, orient='index')
        df_new = pd.DataFrame.from_dict(downtime_data, orient='index')
        
        merged_machines = {}
        unmerged_machines = {}
        
        for machine_id, new_downtime in downtime_data.items():
            if machine_id in last_downtime_data and last_downtime_data[machine_id]["downtime_periods"]:
                last_periods = last_downtime_data[machine_id]["downtime_periods"]
                last_end_time = last_periods[-1]["end_time"]
                last_end_dt = datetime.datetime.strptime(last_end_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                
                if new_downtime["downtime_periods"]:
                    new_start_dt = datetime.datetime.strptime(new_downtime["downtime_periods"][0]["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    time_diff = (new_start_dt - last_end_dt).total_seconds()
                    
                    if time_diff <= 30:
                        last_periods[-1]["end_time"] = new_downtime["downtime_periods"][0]["end_time"]
                        last_periods[-1]["duration_seconds"] += new_downtime["downtime_periods"][0]["duration_seconds"]
                        last_downtime_data[machine_id]["total_downtime_duration"] += new_downtime["total_downtime_duration"]
                        merged_machines[machine_id] = last_downtime_data[machine_id]
                    else:
                        unmerged_machines[machine_id] = new_downtime
                else:
                    unmerged_machines[machine_id] = new_downtime
            else:
                # If the machine wasn't in the previous entry, store it separately
                unmerged_machines[machine_id] = new_downtime
        
        if merged_machines:
            collection.update_one(
                {"_id": last_entry["_id"]},
                {"$set": {"end_time": end_time_ist, "downtime_data": {**last_downtime_data, **merged_machines}}}
            )
            print("Merged downtime where applicable.")
        
        if unmerged_machines:
            data = {
                "timestamp": datetime.datetime.now().isoformat() + 'Z',
                "start_time": start_time_ist,
                "end_time": end_time_ist,
                "downtime_data": unmerged_machines
            }
            collection.insert_one(data)
            print("Stored separate document for unmerged downtimes.")
    else:
        data = {
            "timestamp": datetime.datetime.now().isoformat() + 'Z',
            "start_time": start_time_ist,
            "end_time": end_time_ist,
            "downtime_data": downtime_data
        }
        collection.insert_one(data)
        print("Stored first downtime record.")
    
    client.close()

def main():
    while True:
        now_ist = datetime.datetime.now().replace(second=0, microsecond=0)
        start_time_utc = (now_ist - datetime.timedelta(minutes=30)).isoformat() + 'Z'
        end_time_utc = now_ist.isoformat() + 'Z'

        print(f"Checking downtime for slot: {start_time_utc} to {end_time_utc} IST...")

        all_downtime_data = {machine_id: calculator.calculate_downtime_and_connection_lost(start_time_utc, end_time_utc, machine_id) for machine_id in MACHINE_IDS}

        merge_or_store_downtime(MONGO_URL, DB_NAME, COLLECTION_NAME, all_downtime_data, start_time_utc, end_time_utc)

        print("Sleeping for 30 minutes...")
        time.sleep(1800)

if __name__ == "__main__":
    main()
