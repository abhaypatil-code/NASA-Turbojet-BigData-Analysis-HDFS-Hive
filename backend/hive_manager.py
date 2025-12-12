import subprocess
from backend.config import get_hive_cmd_prefix, HDFS_ROOT, CMAPSS_SCHEMA

class HiveManager:
    def __init__(self):
        self.cmd_prefix = get_hive_cmd_prefix()

    def run_query(self, query):
        """
        Executes a HiveQL query via command line.
        """
        try:
             # Construct command: hive -e "query"
             clean_query = query.replace('\n', ' ').strip()
             cmd_str = " ".join(self.cmd_prefix) + f' "{clean_query}"'
             
             result = subprocess.check_output(cmd_str, shell=True, stderr=subprocess.STDOUT)
             return True, result.decode('utf-8')
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)

    def create_cmapss_tables(self):
        """
        Creates external tables for CMAPSS Train, Test, and RUL datasets.
        """
        # 1. Train Table
        cols = CMAPSS_SCHEMA['columns']
        hive_cols = [f"{c} FLOAT" for c in cols] # Mostly floats
        # unit_number and time_cycles are int conceptually but keeping float is safer for csv flexible read
        
        # Add dataset_id as a regular column since we included it in the CSV
        hive_cols.append("dataset_id STRING")
        
        schema_str = ", ".join(hive_cols)
        
        queries = []
        
        # Drop previous
        queries.append("DROP TABLE IF EXISTS cmapss_train")
        queries.append("DROP TABLE IF EXISTS cmapss_test")
        queries.append("DROP TABLE IF EXISTS cmapss_rul")

        # Create Train
        # Pointing to directory /bda_project/processed/train -> Hive reads all files there
        queries.append(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmapss_train (
            {schema_str}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{HDFS_ROOT}/processed/train';
        """)

        # Create Test (Same Schema)
        queries.append(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmapss_test (
            {schema_str}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{HDFS_ROOT}/processed/test';
        """)

        # Create RUL table
        # Schema: unit_number (implied index 1..N), rul value, dataset_id
        # The raw RUL files are just one column of RULs. 
        # BUT our cleaning script might need to add Unit Number if it's missing?
        # Re-checking RUL files: "112\n98\n..." -> It's just the value. Row 1 = Unit 1.
        # Our Ingestion script needs to be smart enough to add Unit ID to RUL csv if we want it queryable by unit.
        # Let's trust the ingestion script added `dataset_id`. 
        # Wait, `pd.read_csv` on RUL file will give 1 column. 
        # Ideally we should add 'unit_number' column during cleaning of RUL.
        # I will update Hive schema to expect: rul FLOAT, dataset_id STRING.
        # And we can generate Unit ID using `ROW_NUMBER()` in Hive if needed, or update ingestion to add it.
        # Let's keep it simple: RUL, Dataset_ID.
        
        queries.append(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmapss_rul (
            rul FLOAT,
            dataset_id STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{HDFS_ROOT}/processed/rul';
        """)

        results = []
        for q in queries:
            success, msg = self.run_query(q)
            results.append((success, msg))
            
        return results

if __name__ == "__main__":
    hm = HiveManager()
    print("Creating Tables...")
    res = hm.create_cmapss_tables()
    for s, m in res:
        print(f"Status: {s}, Msg: {m.strip() if m else 'OK'}")
