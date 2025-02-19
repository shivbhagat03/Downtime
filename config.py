import os
from dotenv import load_dotenv #type:ignore

load_dotenv()


MQTT_BROKER = "3.7.85.13"
MQTT_PORT = 1883
MQTT_TOPIC = "JBMGroup/MachineData/7891"


INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = "test"
INFLUX_BUCKET = "mq_bucket"
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
if not INFLUXDB_TOKEN:
    raise ValueError("INFLUXDB_TOKEN environment variable is not set")


