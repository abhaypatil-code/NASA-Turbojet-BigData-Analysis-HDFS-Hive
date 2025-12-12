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

def test_mapreduce_inline():
    print("\n--- Testing MapReduce (Inline) ---")
    mrm = MapReduceManager()
    
    # Create a dummy CSV for MR input
    dummy_csv = "test_mr.csv"
    with open(dummy_csv, "w") as f:
        # Create header-like 26 cols
        # Unit Time Op1 Op2 Op3 S1..S21
        # S11 is index 15. Let's put 50.0 there.
        row = ["1", "1", "0", "0", "0"] + ["0"]*21
        row[15] = "50.0" 
        f.write(",".join(row) + "\n")
        row[15] = "60.0"
        f.write(",".join(row) + "\n")
        
    ok, out = mrm.run_job("mr_sensor_stats.py", dummy_csv, runner="inline")
    print(f"Job Result:\n{out}")
    
    if "avg" in str(out) and "55.0" in str(out):
        print("SUCCESS: MapReduce calculated average correctly (55.0).")
    else:
        print("FAILURE: MapReduce output unexpected.")

    if os.path.exists(dummy_csv):
        os.remove(dummy_csv)

if __name__ == "__main__":
    test_mongo()
    test_hdfs_clean_upload()
    test_mapreduce_inline()
