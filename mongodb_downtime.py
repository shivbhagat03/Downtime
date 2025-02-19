from pymongo import MongoClient  #type:ignore
from downtime import DowntimeCalculator
from datetime import datetime,timedelta
import traceback

client = MongoClient("mongodb://localhost:27017/")
db= client["machine_data"]
collection =db["downtime_records"]


def store_downtime_data(start_time,end_time,machineId):

        calculator=DowntimeCalculator()
        data=calculator.calculate_downtime_and_connection_lost(start_time,end_time,machineId)

        if "error" not in data:
            record = collection.find_one({
                "machineId": machineId,
                "downtime_periods":data["downtime_periods"],
                "connection_loss_periods":data["connection_lost_periods"]
                
            })
            if not record:
                collection.insert_one(data)
                print("Downtime stored")
            else:
                print("Duplicate data")
        else:
            print("Error", data["error"])
 
    
if __name__ == "__main__":
    start_time="2025-02-04T10:00:00Z"
    end_time="2025-02-04T17:00:00Z"
    machineId="BAY_1_1100"

    store_downtime_data(start_time, end_time, machineId)