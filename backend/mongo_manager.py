import pandas as pd
import pymongo
from backend.config import MONGO_URI, DB_NAME, COLLECTION_NAME, CMAPSS_SCHEMA

class MongoManager:
    def __init__(self):
        try:
            self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            self.db = self.client[DB_NAME]
            self.collection = self.db[COLLECTION_NAME]
        except Exception as e:
            print(f"MongoDB Connection Warning: {e}")
            self.client = None
            self.db = None
            self.collection = None

    def test_connection(self):
        if not self.client:
             return False, "Client not initialized."
        try:
            self.client.server_info()
            return True, "Connected successfully"
        except Exception as e:
            return False, str(e)

    def ingest_data(self, data):
        """
        Ingests the CMaps dataset. 
        Accepts: 
        - filepath (str)
        - pd.DataFrame
        """
        if not self.collection:
             return False, "MongoDB not connected."

        try:
            df = None
            if isinstance(data, str):
                # Standard ingestion logic from file
                cols = CMAPSS_SCHEMA['columns']
                # If reading direct raw file
                df = pd.read_csv(data, sep=r'\s+', header=None, names=cols, engine='python')
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                return False, "Unsupported data type for ingestion."

            # Drop existing for this batch/dataset if needed, or append? 
            # For this project, we might want to clear specific dataset_id if it exists to avoid dupes
            if 'dataset_id' in df.columns:
                 dataset_ids = df['dataset_id'].unique()
                 self.collection.delete_many({'dataset_id': {'$in': list(dataset_ids)}})

            records = df.to_dict('records')
            
            if records:
                self.collection.insert_many(records)
            
            return True, f"Successfully ingested {len(records)} records."
        except Exception as e:
            return False, f"Ingestion Error: {str(e)}"

    def get_summary(self):
        if not self.collection: return {"status": "Not Connected"}
        count = self.collection.count_documents({})
        if count == 0:
            return {"status": "No Data"}
        
        summary = {"total_records": count}
        summary["total_units"] = len(self.collection.distinct('unit_number'))
        summary["dataset_ids"] = list(self.collection.distinct('dataset_id'))
        summary["data_type"] = "C-MAPSS Timeseries"
        return summary

    def get_sensor_trends(self, unit_id, dataset_id="FD001"):
        if not self.collection: return []
        cursor = self.collection.find(
            {'unit_number': int(unit_id), 'dataset_id': dataset_id}, 
            {'_id': 0}
        ).sort('time_cycles', 1)
        return list(cursor)

    def get_avg_sensors_per_unit(self, dataset_id="FD001"):
        if not self.collection: return []
        pipeline = [
            {"$match": {"dataset_id": dataset_id}},
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
    
    def get_unit_health_scores(self, dataset_id="FD001"):
        """
        Calculates a proxy 'Health Score'.
        """
        if not self.collection: return []
        pipeline = [
            {"$match": {"dataset_id": dataset_id}},
            {"$group": {
                "_id": "$unit_number",
                "max_life": {"$max": "$time_cycles"},
                "avg_pressure": {"$avg": "$sensor_11"},
                "avg_temp": {"$avg": "$sensor_4"}
            }},
            {"$project": {
                "unit_number": "$_id",
                "max_life": 1,
                "health_index": {"$divide": ["$avg_pressure", "$avg_temp"]}
            }},
             {"$sort": {"max_life": 1}}
        ]
        return list(self.collection.aggregate(pipeline))

if __name__ == "__main__":
    mm = MongoManager()
    connected, msg = mm.test_connection()
    print(f"Connection Test: {msg}")

