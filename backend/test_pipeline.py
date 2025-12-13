import sys
import os
import shutil

# Ensure backend can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.mongo_manager import MongoManager
from backend.hdfs_manager import HDFSManager
from backend.mapreduce_manager import MapReduceManager
from backend.config import CMAPS_DIR

def test_mongo():
    print("--- Testing MongoDB ---")
    mm = MongoManager()
    ok, msg = mm.test_connection()
    print(f"Connection: {ok} - {msg}")
    
    # Try ingesting a small file if connection works
    if ok:
        # Looking for a sample file
        files = [f for f in os.listdir(CMAPS_DIR) if f.endswith('001.txt')]
        if files:
            target = os.path.join(CMAPS_DIR, files[0])
            print(f"Ingesting {target}...")
            ok, msg = mm.ingest_data(target)
            print(f"Ingest Result: {ok} - {msg}")
            
            summary = mm.get_summary()
            print(f"Summary: {summary}")
        else:
            print("No FD001.txt found to test ingestion.")

def test_hdfs_clean_upload():
    print("\n--- Testing HDFS Upload & Clean ---")
    hm = HDFSManager()
    
    # Create a dummy file to test cleaning
    dummy_path = "test_raw.txt"
    with open(dummy_path, "w") as f:
        # Variable whitespace
        f.write("1 1 0.001   0.002 100.0 518.67 641.82 1589.70 1400.60 14.62 21.61 554.36 2388.06 9046.19 1.30 47.47 521.66 2388.02 8138.62 8.4195 0.03 392 2388 100.00 39.06 23.4190\n")
    
    # Upload
    dest = "/test_processed/test_clean.csv"
    ok, msg = hm.upload_and_clean_file(dummy_path, dest)
    print(f"Upload Result: {ok} - {msg}")
    
    # If successful, try to cat it to verify comma separation
    if ok:
        ok, content = hm.cat_file(dest)
        print(f"Content Check: {content.strip()}")
        if ',' in str(content):
             print("SUCCESS: File appears to be comma separated.")
        else:
             print("WARNING: File does not appear to be comma separated.")
    
    # Cleanup
    if os.path.exists(dummy_path):
        os.remove(dummy_path)

def test_mapreduce_yarn():
    print("\n--- Testing MapReduce (YARN Cluster) ---")
    print("NOTE: This test requires Docker containers (namenode, resourcemanager, nodemanager) to be running.")
    print("MapReduce jobs now execute exclusively on Hadoop YARN cluster.")
    
    # This test is commented out as it requires:
    # 1. Docker containers running (namenode, resourcemanager, nodemanager)
    # 2. Data ingested to HDFS
    # 3. YARN cluster properly configured
    
    print("To manually test MapReduce:")
    print("  1. Ensure Docker containers are running: docker ps")
    print("  2. Run: streamlit run app.py")
    print("  3. Navigate to MapReduce Jobs tab")
    print("  4. Select a job and provide HDFS input path")
    print("  5. Click 'Run MapReduce Job on YARN Cluster'")
    
    # mrm = MapReduceManager()
    # ok, out = mrm.run_job("mr_sensor_stats.py", "/bda_project/processed/train/FD001.csv", runner="hadoop")
    # print(f"Job Result:\n{out}")

if __name__ == "__main__":
    test_mongo()
    test_hdfs_clean_upload()
    test_mapreduce_yarn()
