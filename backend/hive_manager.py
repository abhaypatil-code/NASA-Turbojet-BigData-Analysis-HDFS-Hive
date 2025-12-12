import subprocess
from backend.config import get_hive_cmd_prefix, HDFS_ROOT

class HiveManager:
    def __init__(self):
        self.cmd_prefix = get_hive_cmd_prefix()

    def run_query(self, query):
        """
        Executes a HiveQL query via command line.
        """
        # cmd = shell_prefix + [query]
        # For simplicity, we assume 'hive -e "query"' structure
        
        cmd = self.cmd_prefix + [f'{query}']
        
        try:
             # On Windows/Subprocess, passing long strings with quotes can be tricky.
             # but list args usually handles it.
             
             # If using docker: docker exec ... hive -e "query"
             # If local: hive -e "query"
             
             # We need to construct the command so subprocess can handle it.
             # If cmd_prefix is ["hive", "-e"], then adding the query string is correct.
             
             cmd_str = " ".join(self.cmd_prefix) + f' "{query}"'
             
             # Using shell=True again for environment robustness
             result = subprocess.check_output(cmd_str, shell=True, stderr=subprocess.STDOUT)
             return True, result.decode('utf-8')
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)

    def create_table(self):
        """
        Creates the external table mapping to the HDFS location of CMaps data.
        Drops table if exists to ensure fresh schema.
        """
        # Note: We assume the data is uploaded to HDFS_ROOT/input/ or similar.
        # The schema matches CMaps: Unit, Time, Op1, Op2, Op3, s1..s21
        
        schema = """
        unit_number INT,
        time_cycles INT,
        op_setting_1 FLOAT,
        op_setting_2 FLOAT,
        op_setting_3 FLOAT,
        s1 FLOAT, s2 FLOAT, s3 FLOAT, s4 FLOAT, s5 FLOAT,
        s6 FLOAT, s7 FLOAT, s8 FLOAT, s9 FLOAT, s10 FLOAT,
        s11 FLOAT, s12 FLOAT, s13 FLOAT, s14 FLOAT, s15 FLOAT,
        s16 FLOAT, s17 FLOAT, s18 FLOAT, s19 FLOAT, s20 FLOAT, s21 FLOAT
        """
        
        drop_query = "DROP TABLE IF EXISTS cmaps_sensors;"
        
        create_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmaps_sensors (
            {schema}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ' '
        STORED AS TEXTFILE
        LOCATION '{HDFS_ROOT}/input';
        """
        
        # Execute Drop
        self.run_query(drop_query)
        
        # Execute Create
        # We need to sanitize newlines for CLI execution
        clean_query = create_query.replace('\n', ' ').strip()
        return self.run_query(clean_query)

if __name__ == "__main__":
    hm = HiveManager()
    print("Hive Manager Initialized")
