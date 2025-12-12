"""
MongoDB Manager for CMAPSS Dataset
Handles all MongoDB operations including CRUD, aggregations, and analytics
Optimized for big data storage with proper indexing and batch operations
"""

import pandas as pd
import pymongo
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, PyMongoError
from backend.config import (
    MONGO_URI, DB_NAME, COLLECTION_NAME, CMAPSS_SCHEMA,
    MONGO_INDEXES, MONGO_BATCH_SIZE, DATASET_METADATA, CRITICAL_SENSORS
)


class MongoManager:
    """Manages MongoDB operations for CMAPSS turbofan engine data"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        self.connect()
    
    def connect(self):
        """Establish connection to MongoDB and create indexes"""
        try:
            self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.db = self.client[DB_NAME]
            self.collection = self.db[COLLECTION_NAME]
            
            # Create indexes for optimized querying
            self._create_indexes()
            
            return True
        except Exception as e:
            print(f"MongoDB Connection Warning: {e}")
            self.client = None
            self.db = None
            self.collection = None
            return False
    
    def _create_indexes(self):
        """Create compound indexes for efficient querying"""
        if self.collection is None:
            return
        
        try:
            # Create compound indexes defined in config
            for index_fields in MONGO_INDEXES:
                self.collection.create_index(index_fields, background=True)
            print("MongoDB indexes created successfully")
        except Exception as e:
            print(f"Index creation warning: {e}")
    
    def test_connection(self):
        """Test MongoDB connection status"""
        if not self.client:
            return False, "Client not initialized."
        try:
            self.client.server_info()
            count = self.collection.count_documents({})
            return True, f"Connected successfully. Total documents: {count:,}"
        except Exception as e:
            return False, str(e)
    
    # ==================== CREATE OPERATIONS ====================
    
    def ingest_data(self, data, batch_size=None):
        """
        Ingest CMAPSS dataset into MongoDB with batch processing
        
        Args:
            data: Either filepath (str) or pandas DataFrame
            batch_size: Number of records per batch (default from config)
        
        Returns:
            tuple: (success, message)
        """
        if self.collection is None:
            return False, "MongoDB not connected."
        
        if batch_size is None:
            batch_size = MONGO_BATCH_SIZE
        
        try:
            df = None
            
            # Load data
            if isinstance(data, str):
                cols = CMAPSS_SCHEMA['columns']
                df = pd.read_csv(data, sep=r'\s+', header=None, names=cols, engine='python')
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                return False, "Unsupported data type for ingestion."
            
            # Remove existing data for this dataset to avoid duplicates
            if 'dataset_id' in df.columns:
                dataset_ids = df['dataset_id'].unique()
                if 'dataset_type' in df.columns:
                    dataset_types = df['dataset_type'].unique()
                    for ds_id in dataset_ids:
                        for ds_type in dataset_types:
                            self.collection.delete_many({
                                'dataset_id': ds_id,
                                'dataset_type': ds_type
                            })
                else:
                    self.collection.delete_many({'dataset_id': {'$in': list(dataset_ids)}})
            
            # Convert to records
            records = df.to_dict('records')
            
            if not records:
                return False, "No records to ingest."
            
            # Batch insertion for better performance
            inserted_count = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    result = self.collection.insert_many(batch, ordered=False)
                    inserted_count += len(result.inserted_ids)
                except BulkWriteError as bwe:
                    # Some records inserted, some failed
                    inserted_count += bwe.details['nInserted']
            
            return True, f"Successfully ingested {inserted_count:,} records in batches of {batch_size}."
            
        except Exception as e:
            return False, f"Ingestion Error: {str(e)}"
    
    # ==================== READ OPERATIONS ====================
    
    def get_dataset(self, dataset_id, dataset_type=None, unit_number=None, limit=None):
        """
        Retrieve dataset with optional filtering
        
        Args:
            dataset_id: Dataset identifier (FD001-FD004)
            dataset_type: 'train' or 'test' (optional)
            unit_number: Specific engine unit (optional)
            limit: Maximum records to return (optional)
        
        Returns:
            list: Records matching criteria
        """
        if self.collection is None:
            return []
        
        query = {'dataset_id': dataset_id}
        if dataset_type:
            query['dataset_type'] = dataset_type
        if unit_number:
            query['unit_number'] = int(unit_number)
        
        cursor = self.collection.find(query, {'_id': 0}).sort([
            ('unit_number', ASCENDING),
            ('time_cycles', ASCENDING)
        ])
        
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    def get_summary(self):
        """Get overall database summary statistics"""
        if self.collection is None:
            return {"status": "Not Connected"}
        
        count = self.collection.count_documents({})
        if count == 0:
            return {"status": "No Data", "total_records": 0}
        
        summary = {
            "total_records": count,
            "total_units": len(self.collection.distinct('unit_number')),
            "dataset_ids": list(self.collection.distinct('dataset_id')),
            "dataset_types": list(self.collection.distinct('dataset_type')),
            "data_type": "C-MAPSS Multivariate Time Series"
        }
        
        # Add per-dataset breakdown
        dataset_breakdown = {}
        for ds_id in summary['dataset_ids']:
            metadata = DATASET_METADATA.get(ds_id, {})
            dataset_breakdown[ds_id] = {
                "train_count": self.collection.count_documents({
                    'dataset_id': ds_id, 
                    'dataset_type': 'train'
                }),
                "test_count": self.collection.count_documents({
                    'dataset_id': ds_id, 
                    'dataset_type': 'test'
                }),
                "description": metadata.get('description', 'N/A'),
                "conditions": metadata.get('operating_conditions', 'N/A'),
                "faults": metadata.get('fault_modes', 'N/A')
            }
        
        summary['datasets'] = dataset_breakdown
        return summary
    
    def get_sensor_trends(self, unit_id, dataset_id="FD001", dataset_type="train"):
        """Get time-series sensor data for a specific engine unit"""
        if self.collection is None:
            return []
        
        cursor = self.collection.find(
            {
                'unit_number': int(unit_id), 
                'dataset_id': dataset_id,
                'dataset_type': dataset_type
            }, 
            {'_id': 0}
        ).sort('time_cycles', ASCENDING)
        
        return list(cursor)
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_sensor_data(self, dataset_id, unit_number, time_cycle, updates):
        """
        Update specific sensor readings
        
        Args:
            dataset_id: Dataset identifier
            unit_number: Engine unit number
            time_cycle: Specific time cycle
            updates: Dictionary of fields to update
        
        Returns:
            tuple: (success, message)
        """
        if self.collection is None:
            return False, "MongoDB not connected."
        
        try:
            result = self.collection.update_one(
                {
                    'dataset_id': dataset_id,
                    'unit_number': int(unit_number),
                    'time_cycles': int(time_cycle)
                },
                {'$set': updates}
            )
            
            if result.matched_count > 0:
                return True, f"Updated {result.modified_count} record(s)"
            else:
                return False, "No matching record found"
                
        except Exception as e:
            return False, f"Update error: {str(e)}"
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_dataset(self, dataset_id, dataset_type=None):
        """
        Delete entire dataset or specific type (train/test)
        
        Args:
            dataset_id: Dataset identifier
            dataset_type: Optional - 'train' or 'test'
        
        Returns:
            tuple: (success, message)
        """
        if self.collection is None:
            return False, "MongoDB not connected."
        
        try:
            query = {'dataset_id': dataset_id}
            if dataset_type:
                query['dataset_type'] = dataset_type
            
            result = self.collection.delete_many(query)
            return True, f"Deleted {result.deleted_count:,} records"
            
        except Exception as e:
            return False, f"Delete error: {str(e)}"
    
    # ==================== ADVANCED AGGREGATION PIPELINES ====================
    
    def get_avg_sensors_per_unit(self, dataset_id="FD001", dataset_type="train"):
        """Calculate average sensor values per engine unit"""
        if self.collection is None:
            return []
        
        # Build aggregation for all sensors
        group_stage = {
            "_id": "$unit_number",
            "max_cycle": {"$max": "$time_cycles"}
        }
        
        # Add averages for all sensors
        for i in range(1, 22):
            sensor_name = f"sensor_{i}"
            group_stage[f"avg_{sensor_name}"] = {"$avg": f"${sensor_name}"}
        
        pipeline = [
            {"$match": {"dataset_id": dataset_id, "dataset_type": dataset_type}},
            {"$group": group_stage},
            {"$sort": {"_id": ASCENDING}},
            {"$project": {
                "unit_number": "$_id",
                "max_life": "$max_cycle",
                **{f"avg_sensor_{i}": f"$avg_sensor_{i}" for i in range(1, 22)},
                "_id": 0
            }}
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def get_unit_health_scores(self, dataset_id="FD001", dataset_type="train"):
        """
        Calculate comprehensive health scores for each engine unit
        Uses multiple sensor indicators to compute overall health index
        """
        if self.collection is None:
            return []
        
        pipeline = [
            {"$match": {"dataset_id": dataset_id, "dataset_type": dataset_type}},
            {"$group": {
                "_id": "$unit_number",
                "max_life": {"$max": "$time_cycles"},
                "avg_temp_lpc": {"$avg": "$sensor_2"},
                "avg_temp_hpc": {"$avg": "$sensor_3"},
                "avg_pressure_hpc": {"$avg": "$sensor_11"},
                "avg_fan_speed": {"$avg": "$sensor_8"},
                "avg_core_speed": {"$avg": "$sensor_9"},
                "std_pressure": {"$stdDevPop": "$sensor_11"}
            }},
            {"$project": {
                "unit_number": "$_id",
                "max_life": 1,
                "avg_temp_lpc": 1,
                "avg_temp_hpc": 1,
                "avg_pressure_hpc": 1,
                # Health index: higher pressure variance indicates degradation
                "health_index": {
                    "$divide": [
                        "$avg_pressure_hpc",
                        {"$add": ["$std_pressure", 1]}  # Avoid division by zero
                    ]
                },
                "_id": 0
            }},
            {"$sort": {"max_life": ASCENDING}}
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def get_sensor_statistics(self, dataset_id="FD001", dataset_type="train", sensors=None):
        """
        Calculate comprehensive statistics for specified sensors
        
        Args:
            dataset_id: Dataset identifier
            dataset_type: 'train' or 'test'
            sensors: List of sensor names (e.g., ['sensor_2', 'sensor_11'])
                    If None, calculates for all sensors
        
        Returns:
            dict: Statistics for each sensor
        """
        if self.collection is None:
            return {}
        
        if sensors is None:
            sensors = [f"sensor_{i}" for i in range(1, 22)]
        
        # Build group stage with statistics for each sensor
        group_stage = {"_id": None}
        for sensor in sensors:
            group_stage[f"{sensor}_min"] = {"$min": f"${sensor}"}
            group_stage[f"{sensor}_max"] = {"$max": f"${sensor}"}
            group_stage[f"{sensor}_avg"] = {"$avg": f"${sensor}"}
            group_stage[f"{sensor}_std"] = {"$stdDevPop": f"${sensor}"}
        
        pipeline = [
            {"$match": {"dataset_id": dataset_id, "dataset_type": dataset_type}},
            {"$group": group_stage}
        ]
        
        result = list(self.collection.aggregate(pipeline))
        return result[0] if result else {}
    
    def get_degradation_trends(self, dataset_id="FD001", dataset_type="train", 
                               window_size=50, sensors=None):
        """
        Analyze sensor degradation trends using time windows
        
        Args:
            dataset_id: Dataset identifier
            dataset_type: 'train' or 'test'
            window_size: Size of cycle window for trend analysis
            sensors: List of sensors to analyze (default: critical sensors)
        
        Returns:
            list: Degradation trend data per unit
        """
        if self.collection is None:
            return []
        
        if sensors is None:
            sensors = CRITICAL_SENSORS[:5]  # Top 5 critical sensors
        
        # This requires calculating rate of change over time windows
        # Simplified version: compare early vs late cycle averages
        pipeline = [
            {"$match": {"dataset_id": dataset_id, "dataset_type": dataset_type}},
            {"$sort": {"unit_number": ASCENDING, "time_cycles": ASCENDING}},
            {"$group": {
                "_id": "$unit_number",
                "total_cycles": {"$max": "$time_cycles"},
                "early_data": {
                    "$push": {
                        "$cond": [
                            {"$lte": ["$time_cycles", window_size]},
                            {sensor: f"${sensor}" for sensor in sensors},
                            "$$REMOVE"
                        ]
                    }
                },
                "late_data": {
                    "$push": {
                        "$cond": [
                            {"$gt": ["$time_cycles", {"$subtract": ["$time_cycles", window_size]}]},
                            {sensor: f"${sensor}" for sensor in sensors},
                            "$$REMOVE"
                        ]
                    }
                }
            }},
            {"$limit": 100}  # Limit for performance
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def get_failure_prediction_features(self, dataset_id="FD001", dataset_type="train"):
        """
        Extract aggregated features useful for RUL prediction
        Includes rolling statistics and sensor interactions
        """
        if self.collection is None:
            return []
        
        pipeline = [
            {"$match": {"dataset_id": dataset_id, "dataset_type": dataset_type}},
            {"$sort": {"unit_number": ASCENDING, "time_cycles": ASCENDING}},
            {"$group": {
                "_id": "$unit_number",
                "total_cycles": {"$max": "$time_cycles"},
                "avg_temp_ratio": {
                    "$avg": {
                        "$divide": [
                            "$sensor_3",  # HPC temp
                            {"$add": ["$sensor_2", 1]}  # LPC temp
                        ]
                    }
                },
                "avg_pressure_ratio": {
                    "$avg": "$sensor_10"  # Engine pressure ratio
                },
                "avg_fan_speed": {"$avg": "$sensor_8"},
                "avg_core_speed": {"$avg": "$sensor_9"},
                "std_fan_speed": {"$stdDevPop": "$sensor_8"},
                "std_core_speed": {"$stdDevPop": "$sensor_9"},
                # Fuel efficiency proxy
                "avg_fuel_ratio": {"$avg": "$sensor_12"}
            }},
            {"$project": {
                "unit_number": "$_id",
                "total_cycles": 1,
                "avg_temp_ratio": 1,
                "avg_pressure_ratio": 1,
                "speed_ratio": {
                    "$divide": [
                        "$avg_core_speed",
                        {"$add": ["$avg_fan_speed", 1]}
                    ]
                },
                "speed_variability": {
                    "$add": ["$std_fan_speed", "$std_core_speed"]
                },
                "avg_fuel_ratio": 1,
                "_id": 0
            }},
            {"$sort": {"unit_number": ASCENDING}}
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def get_condition_based_metrics(self, dataset_id="FD002", dataset_type="train"):
        """
        Analyze performance metrics grouped by operational conditions
        Useful for datasets with multiple operating conditions (FD002, FD004)
        """
        if self.collection is None:
            return []
        
        # Group by operational setting ranges to identify conditions
        pipeline = [
            {"$match": {"dataset_id": dataset_id, "dataset_type": dataset_type}},
            {"$project": {
                "unit_number": 1,
                "time_cycles": 1,
                # Discretize operational settings
                "op_condition": {
                    "$concat": [
                        {"$toString": {"$round": ["$op_setting_1", 0]}},
                        "-",
                        {"$toString": {"$round": ["$op_setting_2", 1]}},
                        "-",
                        {"$toString": {"$round": ["$op_setting_3", 0]}}
                    ]
                },
                "sensor_11": 1,
                "sensor_12": 1,
                "sensor_14": 1
            }},
            {"$group": {
                "_id": "$op_condition",
                "engine_count": {"$addToSet": "$unit_number"},
                "avg_pressure": {"$avg": "$sensor_11"},
                "avg_fuel_ratio": {"$avg": "$sensor_12"},
                "avg_core_speed": {"$avg": "$sensor_14"},
                "total_observations": {"$sum": 1}
            }},
            {"$project": {
                "operational_condition": "$_id",
                "unique_engines": {"$size": "$engine_count"},
                "avg_pressure": 1,
                "avg_fuel_ratio": 1,
                "avg_core_speed": 1,
                "total_observations": 1,
                "_id": 0
            }},
            {"$sort": {"unique_engines": DESCENDING}}
        ]
        
        return list(self.collection.aggregate(pipeline))


if __name__ == "__main__":
    # Test MongoDB Manager
    mm = MongoManager()
    connected, msg = mm.test_connection()
    print(f"Connection Test: {msg}")
    
    if connected:
        print("\n=== Summary ===")
        summary = mm.get_summary()
        print(summary)
