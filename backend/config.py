import os

# Base Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CMAPS_DIR = os.path.join(BASE_DIR, "CMaps")

# MongoDB Config
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "BDA_Project"
COLLECTION_NAME = "sensors"

# HDFS Config
# If using Docker, set USE_DOCKER = True and specify container name
USE_DOCKER = False
NAMENODE_CONTAINER = "namenode" # Common name for HDFS container
HDFS_ROOT = "/bda_project"

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
