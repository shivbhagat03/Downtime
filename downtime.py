from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
from flask import Flask,jsonify,request
import traceback
from config import INFLUX_BUCKET,INFLUX_ORG,INFLUX_URL,INFLUXDB_TOKEN
import pandas as pd

class DowntimeCalculator:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUXDB_TOKEN, org=INFLUX_ORG)
        self.query_api = self.client.query_api()
        

    def calculate_downtime_and_connection_lost(self, start_time, end_time, machineId):
        try:
            start_time_str = start_time
            end_time_str = end_time

            downtime_query = f'''
                from(bucket:"{INFLUX_BUCKET}")
                    |> range(start: time(v:"{start_time_str}"), stop: time(v:"{end_time_str}"))
                    |> filter(fn: (r) => r["_measurement"] == "machine_strokes")
                    |> filter(fn: (r) => r["_field"] == "totalStrokeCounter")
                    |> filter(fn: (r) => r["machineId"] == "{machineId}")
                    |> difference(nonNegative: true, columns: ["_value"])
                    |> map(fn: (r) => ({{
                        r with _value: if r._value == 0.0 then 0.0 else 1.0
                    }}))
                    |> yield(name: "status")    
            '''
            result = self.query_api.query(query=downtime_query,org=INFLUX_ORG)

            downtime_slots = []
            total_duration = 0
            downtime_count = 0
            connection_lost_periods = []
            connection_lost_duration = 0
            records = []  

            all_timestamps = []
            for table in result:
                for record in table.records:
                    all_timestamps.append(record.get_time())
                    records.append({"time": record.get_time(), "value": record.get_value()})

            if all_timestamps:
                df=pd.DataFrame(all_timestamps,columns=['timestamp'])
                df['timestamp']= pd.to_datetime(df['timestamp'])
                df['diff']=df['timestamp'].diff().dt.total_seconds()

                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                if (df['timestamp'].iloc[0] - start_dt).total_seconds() > 120:
                    duration = (df['timestamp'].iloc[0] - start_dt).total_seconds()
                    connection_lost_periods.append({
                        "start_time": start_dt.isoformat().replace('+00:00', 'Z'),
                        "end_time": df['timestamp'].iloc[0].isoformat().replace('+00:00', 'Z'),
                        "duration_seconds": duration
                    })
                    connection_lost_duration += duration

                for i in range(len(df) - 1):
                    gap = df['diff'].iloc[i + 1]
                    if gap > 120:
                        connection_lost_periods.append({
                            "start_time": df['timestamp'].iloc[i].isoformat().replace('+00:00', 'Z'),
                            "end_time": df['timestamp'].iloc[i + 1].isoformat().replace('+00:00', 'Z'),
                            "duration_seconds": gap
                        })
                        connection_lost_duration += gap

                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                if (end_dt - df['timestamp'].iloc[-1]).total_seconds() > 120:
                    duration = (end_dt - df['timestamp'].iloc[-1]).total_seconds()
                    connection_lost_periods.append({
                        "start_time": df['timestamp'].iloc[-1].isoformat().replace('+00:00', 'Z'),
                        "end_time": end_dt.isoformat().replace('+00:00', 'Z'),
                        "duration_seconds": duration
                    })
                    connection_lost_duration += duration
            
            df_downtime=pd.DataFrame(records)
            if not df_downtime.empty:
                df_downtime['is_downtime'] = (df_downtime['value'] == 0.0)
                df_downtime['downtime_group'] = (df_downtime['is_downtime'] != df_downtime['is_downtime'].shift()).cumsum()
                downtime_groups = df_downtime[df_downtime['is_downtime']].groupby('downtime_group')

                downtime_slots = [
                    {
                        "start_time": group['time'].iloc[0].isoformat().replace('+00:00', 'Z'),
                        "end_time": group['time'].iloc[-1].isoformat().replace('+00:00', 'Z'),
                        "duration_seconds": (group['time'].iloc[-1] - group['time'].iloc[0]).total_seconds()
                    }
                    for _, group in downtime_groups if (group['time'].iloc[-1] - group['time'].iloc[0]).total_seconds() > 120
                ]
                total_duration = sum(slot["duration_seconds"] for slot in downtime_slots)
                downtime_count = len(downtime_slots)
            
            return {
                "machineId": machineId,
                "downtime_periods": downtime_slots,
                "total_downtime_duration": total_duration,
                "downtime_count": downtime_count,
                "connection_lost_periods": connection_lost_periods,
                "total_connection_lost_duration": connection_lost_duration,
                "connection_lost_count": len(connection_lost_periods)
            }
        except Exception as e:
            print(f"Error calculating downtime and connection lost: {e}")
            print(traceback.format_exc())
            return {"error": str(e)}  

    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
                    


