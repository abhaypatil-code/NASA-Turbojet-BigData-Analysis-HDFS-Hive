import os

# Base Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CMAPS_DIR = os.path.join(BASE_DIR, "CMAPSS")

# MongoDB Config
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "BDA_Project"
COLLECTION_NAME = "sensors"

# HDFS Config
# If using Docker, set USE_DOCKER = True and specify container name
USE_DOCKER = True
NAMENODE_CONTAINER = "namenode" # Common name for HDFS container
HDFS_ROOT = "/bda_project"

# DATASET SCHEMA DEFINITION
# Based on CMaps/readme.txt
# Columns: Unit, Time, Op1, Op2, Op3, Sensor1...Sensor21
CMAPSS_SCHEMA = {
    "columns": [
        "unit_number",
        "time_cycles",
        "op_setting_1",
        "op_setting_2", 
        "op_setting_3"
    ] + [f"sensor_{i}" for i in range(1, 22)],
    "description": {
        "unit_number": "Unit ID",
        "time_cycles": "Time (Cycles)",
        "op_setting_1": "Operational Setting 1",
        "op_setting_2": "Operational Setting 2",
        "op_setting_3": "Operational Setting 3",
        **{f"sensor_{i}": f"Sensor Measurement {i}" for i in range(1, 22)}
    }
}

# Commands
# If USE_DOCKER is True, commands will be prefixed with 'docker exec ...'
def get_hdfs_cmd_prefix():
    if USE_DOCKER:
        return ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs"]
    return ["hdfs", "dfs"]

def get_hive_cmd_prefix():
    if USE_DOCKER:
        return ["docker", "exec", "hive-server", "hive", "-e"] 
    return ["hive", "-e"]

def get_column_names():
    return CMAPSS_SCHEMA["columns"]
