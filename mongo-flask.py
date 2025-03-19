from flask import Flask, request, jsonify
from pymongo import MongoClient                 # type: ignore

app = Flask(__name__)

# MongoDB Configuration
MONGO_URL = "mongodb://localhost:27017/"
DB_NAME = "machine_data"
COLLECTION_NAME = "data_downtime"

client = MongoClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@app.route("/fetch_downtime", methods=["GET"])
def fetch_downtime():
    """Fetches downtime records for a specific machine in a given time range."""
    
    machine_id = request.args.get("machine_id")  
    start_time = request.args.get("start_time")  
    end_time = request.args.get("end_time")      

    if not machine_id or not start_time or not end_time:
        return jsonify({"error": "Missing required parameters (machine_id, start_time, end_time)"}), 400

    # MongoDB Query to fetch records within the time range
    query = {
        "start_time": {"$gte": start_time},
        "end_time": {"$lte": end_time},
        f"downtime_data.{machine_id}": {"$exists": True}  # Ensure machineId exists
    }

    print("Executing Query:", query)  # Debugging Step

    results = list(collection.find(query, {"_id": 0, f"downtime_data.{machine_id}": 1}))

    if not results:
        return jsonify({"message": "No downtime records found for the given machine in the specified time range."}), 404

    # Extract relevant machine downtime data
    response = {
        "machineId": machine_id,
        "downtime_records": [record["downtime_data"][machine_id] for record in results]
    }

    return jsonify(response), 200

if __name__ == "__main__":
    app.run(debug=True)
