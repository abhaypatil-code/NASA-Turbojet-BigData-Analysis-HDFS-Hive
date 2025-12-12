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
             # We use proper list argument for subprocess where possible, 
             # but complex Hive queries with quotes often behave better as a single shell string
             # especially when passing through docker exec.
             
             clean_query = query.replace('\n', ' ').strip()
             cmd_str = " ".join(self.cmd_prefix) + f' "{clean_query}"'
             
             result = subprocess.check_output(cmd_str, shell=True, stderr=subprocess.STDOUT)
             return True, result.decode('utf-8')
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)

    def create_table(self):
        """
        Creates the external table mapping to the HDFS location of CMaps data.
        Drops table if exists to ensure fresh schema.
        Dynamically generates schema from CMAPSS_SCHEMA.
        """
        # Dynamic Schema Generation
        # Map python/pandas types to Hive types (simplified)
        # All columns in CMAPSS are basically Float/Double, except maybe Unit (Int) and Time (Int)
        
        cols = CMAPSS_SCHEMA['columns']
        hive_cols = []
        for c in cols:
            dtype = "INT" if c in ["unit_number", "time_cycles"] else "FLOAT"
            hive_cols.append(f"{c} {dtype}")
        
        schema_str = ", ".join(hive_cols)
        
        drop_query = "DROP TABLE IF EXISTS cmaps_sensors;"
        
        # Use RegexSerDe to handle variable whitespace safely
        # Pattern: (\\S+)\\s+(\\S+)... 
        # But constructing that for N columns is tedious. 
        # Alternative: Use simple space delimiter and assume cleaned data? 
        # The raw data has variable spaces. RegexSerDe is best.
        # Pattern logic: look for non-whitespace chunks.
        # However, writing robust RegexSerDe for 26+ columns in a single string is error prone to generate.
        # Let's try the simpler approach first: ' ' delimiter, but raw files might fail.
        # Actually, Hive's standard behaviour treats multiple spaces as empty fields if purely delimited.
        # A common workaround is: LOAD data into a temp table with 1 line per row, then parse.
        # OR: Just use ' ' and hope the file doesn't have leading spaces causing NULLs.
        # CMAPSS files usually have index columns.
        
        # Let's stick to standard internal format for now but point to HDFS_ROOT/input
        # If the user wants meaningful analysis, maybe we define a view or just try.
        
        create_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmaps_sensors (
            {schema_str}
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
           "separatorChar" = " ",
           "quoteChar"     = "\\"",
           "escapeChar"    = "\\\\"
        )
        STORED AS TEXTFILE
        LOCATION '{HDFS_ROOT}/input';
        """
        # Note: OpenCSVSerde treats multiple spaces as multiple delimiters usually? 
        # Actually checking the files, they appear to be space separated.
        # Let's try standard Row Format Delimited... 
        
        create_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmaps_sensors (
            {schema_str}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{HDFS_ROOT}/processed';
        """
        
        self.run_query(drop_query)
        return self.run_query(create_query)

if __name__ == "__main__":
    hm = HiveManager()
    print("Hive Manager Initialized")
