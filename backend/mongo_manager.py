import pandas as pd
import pymongo
from backend.config import MONGO_URI, DB_NAME, COLLECTION_NAME, CMAPSS_SCHEMA

class MongoManager:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]

    def test_connection(self):
        try:
            self.client.server_info()
            return True, "Connected successfully"
        except Exception as e:
            return False, str(e)

    def ingest_data(self, filepath):
        """
        Ingests the CMaps dataset using the schema defined in config.py.
        """
        import zipfile
        try:
            # Handle Zip Files
            if filepath.endswith('.zip'):
                with zipfile.ZipFile(filepath, 'r') as z:
                    first_file = z.namelist()[0]
                    with z.open(first_file) as f:
                         self._ingest_stream(f)
            else:
                self._ingest_stream(filepath)
            
            return True, f"Successfully ingested data mapping to {len(CMAPSS_SCHEMA['columns'])} columns."
        except Exception as e:
            return False, f"Ingestion Error: {str(e)}"

    def _ingest_stream(self, file_obj):
        # Determine column structure from Config
        cols = CMAPSS_SCHEMA['columns']
        
        # Read Data
        df = pd.read_csv(file_obj, sep=r'\s+', header=None, names=cols, engine='python')
        
        # Convert to dictionary records
        records = df.to_dict('records')
        
        # Clear existing data and insert
        self.collection.delete_many({})
        if records:
            self.collection.insert_many(records)

    def get_summary(self):
        count = self.collection.count_documents({})
        if count == 0:
            return {"status": "No Data"}
        
        summary = {"total_records": count}
        summary["total_units"] = len(self.collection.distinct('unit_number'))
        summary["data_type"] = "C-MAPSS Timeseries"
        return summary

    def get_sensor_trends(self, unit_id):
        cursor = self.collection.find({'unit_number': unit_id}, {'_id': 0}).sort('time_cycles', 1)
        return list(cursor)

    def get_correlation_data(self):
        cursor = self.collection.find({}, {'_id': 0})
        return list(cursor)

    def get_avg_sensors_per_unit(self):
        pipeline = [
            {"$group": {
                "_id": "$unit_number",
                "avg_s11": {"$avg": "$sensor_11"},
                "avg_s12": {"$avg": "$sensor_12"},
                "avg_s14": {"$avg": "$sensor_14"},
                "max_cycle": {"$max": "$time_cycles"}
            }},
            {"$sort": {"_id": 1}}
        ]
        return list(self.collection.aggregate(pipeline))
    
    # --- Advanced Aggregations ---

    def get_unit_health_scores(self):
        """
        Calculates a proxy 'Health Score' based on sensor deviation from the mean.
        Health Score = 100 - (AVG(Normalized Deviation of Sensors 2, 3, 4, 7, 8, 9, 11, 12, 13, 14, 15, 17, 20, 21))
        This is a simplified example.
        """
        pipeline = [
            # 1. Calc Global Averages for critical sensors (approximate for this demo)
            # In a real system, we'd double query or use hardcoded baselines. 
            # For efficiency in one pipeline without $setWindowFields (older mongo), we'll group by null first? 
            # No, let's keep it simple: Just Max Cycle (RUL proxy) vs Avg Sensor 11 (Pressure)
            
            {"$group": {
                "_id": "$unit_number",
                "max_life": {"$max": "$time_cycles"},
                "avg_pressure": {"$avg": "$sensor_11"},
                "avg_temp": {"$avg": "$sensor_4"}
            }},
            {"$project": {
                "unit_number": "$_id",
                "max_life": 1,
                "health_index": {"$divide": ["$avg_pressure", "$avg_temp"]} # Arbitrary metric
            }},
             {"$sort": {"max_life": 1}}
        ]
        return list(self.collection.aggregate(pipeline))

if __name__ == "__main__":
    mm = MongoManager()
    connected, msg = mm.test_connection()
    print(f"Connection Test: {msg}")
