import os
import pandas as pd
from backend.hdfs_manager import HDFSManager
from backend.mongo_manager import MongoManager
from backend.config import CMAPS_DIR, CMAPSS_SCHEMA
import tempfile

class DataIngestion:
    def __init__(self):
        self.hdfs = HDFSManager()
        self.mongo = MongoManager()
        self.processed_dir = "/bda_project/processed"
        self.hdfs.mkdir(self.processed_dir)
        self.hdfs.mkdir(f"{self.processed_dir}/train")
        self.hdfs.mkdir(f"{self.processed_dir}/test")
        self.hdfs.mkdir(f"{self.processed_dir}/rul")

    def process_and_upload(self):
        """
        Iterates over CMAPSS files, cleans them, adds dataset_id, and uploads to specific HDFS folders.
        Also ingests into MongoDB.
        """
        files = [f for f in os.listdir(CMAPS_DIR) if f.startswith("train_") or f.startswith("test_") or f.startswith("RUL_")]
        
        status_report = []
        
        # Test Mongo Connection
        conn, msg = self.mongo.test_connection()
        if not conn:
            status_report.append(f"WARNING: MongoDB Connection Failed: {msg}. Continuing with HDFS only.")

        for filename in files:
            try:
                local_path = os.path.join(CMAPS_DIR, filename)
                dataset_id = filename.split("_")[1].split(".")[0] # e.g., FD001
                
                # Determine type
                if filename.startswith("train_"):
                    table_type = "train"
                    columns = CMAPSS_SCHEMA['columns']
                elif filename.startswith("test_"):
                    table_type = "test"
                    columns = CMAPSS_SCHEMA['columns']
                elif filename.startswith("RUL_"):
                    table_type = "rul"
                    columns = ["rul"]
                else:
                    continue

                print(f"Processing {filename}...")

                # Read with variable whitespace
                df = pd.read_csv(local_path, sep=r'\s+', header=None, engine='python')
                
                # Filter columns if necessary (sometimes extra empty col at end due to trailing space)
                df = df.iloc[:, :len(columns)]
                df.columns = columns
                
                # Add dataset_id column
                df['dataset_id'] = dataset_id
                df['dataset_type'] = table_type

                # 1. MongoDB Ingestion
                if conn:
                    # Ingest only if we haven't already (simple check could be added here, but MongoManager handles upsert/delete-insert)
                    success, msg = self.mongo.ingest_data(df)
                    if success:
                        status_report.append(f"MONGO SUCCESS: {filename} -> {msg}")
                    else:
                        status_report.append(f"MONGO FAIL: {filename} -> {msg}")

                # 2. HDFS Ingestion
                # Save to temporary CSV using tempfile for safety
                fd, temp_csv = tempfile.mkstemp(suffix=f"_{filename}.csv")
                os.close(fd)
                
                try:
                    # Save without header for Hive compatibility
                    df.to_csv(temp_csv, index=False, header=False)

                    # Upload to HDFS
                    hdfs_dest = f"{self.processed_dir}/{table_type}/{dataset_id}.csv"
                    success, msg = self.hdfs.upload_file(temp_csv, hdfs_dest)
                    
                    if success:
                        status_report.append(f"HDFS SUCCESS: {filename} -> {hdfs_dest}")
                    else:
                        status_report.append(f"HDFS FAILURE: {filename} -> {msg}")
                        
                finally:
                    # Clean up local temp
                    if os.path.exists(temp_csv):
                        os.remove(temp_csv)

            except Exception as e:
                status_report.append(f"ERROR: {filename} -> {str(e)}")
        
        return "\n".join(status_report)

if __name__ == "__main__":
    di = DataIngestion()
    print("Starting Ingestion...")
    report = di.process_and_upload()
    print(report)
