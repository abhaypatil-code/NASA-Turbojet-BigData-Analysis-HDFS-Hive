import pandas as pd
import pymongo
from backend.config import MONGO_URI, DB_NAME, COLLECTION_NAME

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
        Ingests the CMaps dataset (space separated text file).
        """
        # Column names based on C-MAPSS documentation
        # Unit, Time, Op1, Op2, Op3, Sensor1 ... Sensor21
        cols = ['unit_number', 'time_cycles', 'op_setting_1', 'op_setting_2', 'op_setting_3'] + \
               [f'sensor_{i}' for i in range(1, 22)]
        
        try:
            # Read file using pandas
            df = pd.read_csv(filepath, sep='\s+', header=None, names=cols)
            
            # Convert to dictionary records
            records = df.to_dict('records')
            
            # Clear existing data to avoid duplicates for this run
            self.collection.delete_many({})
            
            # Insert
            self.collection.insert_many(records)
            return True, f"Successfully inserted {len(records)} records."
        except Exception as e:
            return False, str(e)

    def get_summary(self):
        count = self.collection.count_documents({})
        if count == 0:
            return {"status": "No Data"}
        
        # Aggregation: Count unique units
        unique_units = len(self.collection.distinct('unit_number'))
        
        return {
            "total_records": count,
            "total_units": unique_units
        }

    def get_sensor_trends(self, unit_id):
        """
        Get sensor data for a specific unit over time.
        """
        cursor = self.collection.find({'unit_number': unit_id}, {'_id': 0}).sort('time_cycles', 1)
        return list(cursor)

    def get_correlation_data(self):
        """
        Get all data for correlation analysis in Pandas.
        Limit to first 5000 records if too large for quick memory, 
        or full dataset if manageable. (CMaps is manageable ~10MB)
        """
        cursor = self.collection.find({}, {'_id': 0})
        return list(cursor)

    def get_avg_sensors_per_unit(self):
        """
        Uses MongoDB Aggregation Pipeline to calculate average sensor readings per unit.
        """
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

if __name__ == "__main__":
    mm = MongoManager()
    connected, msg = mm.test_connection()
    print(f"Connection Test: {msg}")
