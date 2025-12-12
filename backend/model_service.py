import os
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from backend.hdfs_manager import HDFSManager
from backend.mongo_manager import MongoManager
from backend.config import CMAPSS_SCHEMA, BASE_DIR

class ModelService:
    def __init__(self):
        self.hdfs = HDFSManager()
        self.mongo = MongoManager()
        self.models_dir = os.path.join(BASE_DIR, "models")
        if not os.path.exists(self.models_dir):
            os.makedirs(self.models_dir)

    def _get_data(self, dataset_id, table_type="train"):
        """
        Fetches data from MongoDB (preferred) or HDFS.
        """
        # Try Mongo first
        conn, _ = self.mongo.test_connection()
        if conn:
            print(f"Fetching {table_type} data for {dataset_id} from MongoDB...")
            try:
                # Query MongoDB for this dataset_id and type
                # We expect the ingestion to have stored 'dataset_type' as 'train' or 'test'
                pipeline = [
                    {"$match": {"dataset_id": dataset_id, "dataset_type": table_type}},
                    {"$project": {"_id": 0}} # Exclude Mongo ID
                ]
                
                # Use aggregate or find. Find is simpler if we just want all records.
                # However, sorting is important for time series.
                cursor = self.mongo.collection.find(
                    {"dataset_id": dataset_id, "dataset_type": table_type},
                    {"_id": 0}
                ).sort([("unit_number", 1), ("time_cycles", 1)])
                
                data = list(cursor)
                if data:
                    return pd.DataFrame(data)
                else:
                    print(f"No data found in MongoDB for {dataset_id} ({table_type}). Fallback to HDFS.")
            except Exception as e:
                print(f"MongoDB Fetch Error: {e}. Fallback to HDFS.")

        # Fallback to HDFS
        hdfs_path = f"/bda_project/processed/{table_type}/{dataset_id}.csv"
        return self._get_data_from_hdfs(hdfs_path)

    def _get_data_from_hdfs(self, hdfs_path):
        """
        Downloads CSV from HDFS to a temp file and reads it into DataFrame.
        """
        filename = os.path.basename(hdfs_path)
        local_path = os.path.join(self.models_dir, f"temp_{filename}")
        
        success, msg = self.hdfs.download_file(hdfs_path, local_path)
        if not success:
            raise Exception(f"Failed to download {hdfs_path}: {msg}")
        
        # Read CSV (No header as per our ingestion script)
        # We need to manually assign column names
        # Ingestion adds 'dataset_id' at the end.
        cols = CMAPSS_SCHEMA['columns'] + ['dataset_id']
        
        df = pd.read_csv(local_path, header=None, names=cols)
        
        if os.path.exists(local_path):
            os.remove(local_path)
            
        return df

    def prepare_training_data(self, df):
        """
        Calculates RUL for training data.
        RUL = Max Cycle - Current Cycle (for each unit)
        """
        # Calculate max cycle per unit
        max_cycles = df.groupby('unit_number')['time_cycles'].max().reset_index()
        max_cycles.columns = ['unit_number', 'max_cycle']
        
        # Merge back
        df = df.merge(max_cycles, on='unit_number', how='left')
        
        # Calculate RUL
        df['RUL'] = df['max_cycle'] - df['time_cycles']
        
        features = ['op_setting_1', 'op_setting_2', 'op_setting_3'] + \
                   [f'sensor_{i}' for i in range(1, 22)]
        
        X = df[features]
        y = df['RUL']
        
        return X, y

    def train_model(self, dataset_id="FD001"):
        """
        Trains a Random Forest Regressor for a specific dataset ID.
        """
        try:
            print(f"Fetching training data for {dataset_id}...")
            df = self._get_data(dataset_id, "train")
            
            if df.empty:
                return False, "Dataframe is empty."
            
            print("Preparing training data...")
            X, y = self.prepare_training_data(df)
            
            print(f"Training Random Forest for {dataset_id}...")
            model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
            model.fit(X, y)
            
            # Save model
            model_path = os.path.join(self.models_dir, f"rul_model_{dataset_id}.pkl")
            joblib.dump(model, model_path)
            
            print(f"Model saved to {model_path}")
            return True, f"Model trained and saved: {model_path}"
            
        except Exception as e:
            return False, str(e)

    def predict_rul(self, dataset_id, unit_number=None):
        """
        Runs prediction on the Test set for a specific Dataset ID.
        If unit_number is provided, returns prediction series for that unit.
        """
        try:
            # Load model
            model_path = os.path.join(self.models_dir, f"rul_model_{dataset_id}.pkl")
            if not os.path.exists(model_path):
                return None, "Model not found. Please train first."
            
            model = joblib.load(model_path)
            
            # Load Test Data
            df_test = self._get_data(dataset_id, "test")
            
            if unit_number:
                df_test = df_test[df_test['unit_number'] == int(unit_number)]
            
            if df_test.empty:
                return None, "No test data found."
            
            features = ['op_setting_1', 'op_setting_2', 'op_setting_3'] + \
                       [f'sensor_{i}' for i in range(1, 22)]
                       
            X_test = df_test[features]
            predictions = model.predict(X_test)
            
            # Attach predictions
            df_test['predicted_rul'] = predictions
            
            # Return result
            return df_test[['unit_number', 'time_cycles', 'predicted_rul']], "Success"
            
        except Exception as e:
            return None, str(e)

if __name__ == "__main__":
    ms = ModelService()
    # Manual test trigger if run directly
    # print(ms.train_model("FD001"))

