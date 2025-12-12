import os

# Base Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CMAPS_DIR = os.path.join(BASE_DIR, "CMAPSS")

# ==================== MONGODB CONFIGURATION ====================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "BDA_Project"
COLLECTION_NAME = "sensors"

# MongoDB Indexing Configuration
MONGO_INDEXES = [
    # Compound index for efficient querying by dataset and unit
    [("dataset_id", 1), ("unit_number", 1), ("time_cycles", 1)],
    # Index for dataset type filtering
    [("dataset_type", 1), ("dataset_id", 1)],
    # Index for time-series queries
    [("unit_number", 1), ("time_cycles", 1)],
]

# MongoDB Batch Configuration
MONGO_BATCH_SIZE = 1000  # Records per batch for bulk insertion

# ==================== HDFS CONFIGURATION ====================
# If using Docker, set USE_DOCKER = True and specify container name
USE_DOCKER = True
NAMENODE_CONTAINER = "namenode"  # Common name for HDFS container
DATANODE_CONTAINER = "datanode"
HIVE_SERVER_CONTAINER = "hive-server"
HDFS_ROOT = "/bda_project"

# HDFS Directory Structure
HDFS_DIRS = {
    "root": HDFS_ROOT,
    "processed": f"{HDFS_ROOT}/processed",
    "train": f"{HDFS_ROOT}/processed/train",
    "test": f"{HDFS_ROOT}/processed/test",
    "rules": f"{HDFS_ROOT}/processed/rul",
    "mapreduce_output": f"{HDFS_ROOT}/mapreduce_output",
    "models": f"{HDFS_ROOT}/models",
    "uploads": f"{HDFS_ROOT}/uploads",
}

# ==================== DATASET SCHEMA & METADATA ====================
# Based on CMAPSS/readme.txt
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
        "unit_number": "Engine Unit ID",
        "time_cycles": "Operational Cycle (Time)",
        "op_setting_1": "Operational Setting 1 (Altitude)",
        "op_setting_2": "Operational Setting 2 (Mach Number)",
        "op_setting_3": "Operational Setting 3 (Throttle Resolver Angle)",
        **{f"sensor_{i}": f"Sensor Measurement {i}" for i in range(1, 22)}
    },
    "sensor_names": {
        "sensor_1": "Total temperature at fan inlet (°R)",
        "sensor_2": "Total temperature at LPC outlet (°R)",
        "sensor_3": "Total temperature at HPC outlet (°R)",
        "sensor_4": "Total temperature at LPT outlet (°R)",
        "sensor_5": "Pressure at fan inlet (psia)",
        "sensor_6": "Total pressure in bypass-duct (psia)",
        "sensor_7": "Total pressure at HPC outlet (psia)",
        "sensor_8": "Physical fan speed (rpm)",
        "sensor_9": "Physical core speed (rpm)",
        "sensor_10": "Engine pressure ratio (P50/P2)",
        "sensor_11": "Static pressure at HPC outlet (psia)",
        "sensor_12": "Ratio of fuel flow to Ps30 (pps/psi)",
        "sensor_13": "Corrected fan speed (rpm)",
        "sensor_14": "Corrected core speed (rpm)",
        "sensor_15": "Bypass Ratio",
        "sensor_16": "Burner fuel-air ratio",
        "sensor_17": "Bleed Enthalpy",
        "sensor_18": "Demanded fan speed (rpm)",
        "sensor_19": "Demanded corrected fan speed (rpm)",
        "sensor_20": "HPT coolant bleed (lbm/s)",
        "sensor_21": "LPT coolant bleed (lbm/s)",
    }
}

# Dataset Metadata - Characteristics of each CMAPSS dataset
DATASET_METADATA = {
    "FD001": {
        "name": "FD001",
        "train_engines": 100,
        "test_engines": 100,
        "operating_conditions": 1,
        "fault_modes": 1,
        "description": "Single operating condition, single fault mode (HPC degradation)",
        "train_file": "train_FD001.txt",
        "test_file": "test_FD001.txt",
        "rul_file": "RUL_FD001.txt",
    },
    "FD002": {
        "name": "FD002",
        "train_engines": 260,
        "test_engines": 259,
        "operating_conditions": 6,
        "fault_modes": 1,
        "description": "Six operating conditions, single fault mode (HPC degradation)",
        "train_file": "train_FD002.txt",
        "test_file": "test_FD002.txt",
        "rul_file": "RUL_FD002.txt",
    },
    "FD003": {
        "name": "FD003",
        "train_engines": 100,
        "test_engines": 100,
        "operating_conditions": 1,
        "fault_modes": 2,
        "description": "Single operating condition, two fault modes (HPC & Fan degradation)",
        "train_file": "train_FD003.txt",
        "test_file": "test_FD003.txt",
        "rul_file": "RUL_FD003.txt",
    },
    "FD004": {
        "name": "FD004",
        "train_engines": 248,
        "test_engines": 249,
        "operating_conditions": 6,
        "fault_modes": 2,
        "description": "Six operating conditions, two fault modes (HPC & Fan degradation)",
        "train_file": "train_FD004.txt",
        "test_file": "test_FD004.txt",
        "rul_file": "RUL_FD004.txt",
    }
}

# Critical sensors for health monitoring (based on domain knowledge)
CRITICAL_SENSORS = ["sensor_2", "sensor_3", "sensor_4", "sensor_7", "sensor_8", 
                    "sensor_9", "sensor_11", "sensor_12", "sensor_13", "sensor_14", "sensor_15"]

# ==================== MAPREDUCE CONFIGURATION ====================
MAPREDUCE_CONFIG = {
    "jobs": {
        "cycle_counter": {
            "script": "mr_cycle_counter.py",
            "description": "Count total cycles per engine unit",
            "output_dir": f"{HDFS_ROOT}/mapreduce_output/cycle_counts"
        },
        "feature_summary": {
            "script": "mr_feature_summary.py",
            "description": "Calculate statistics for all 26 features",
            "output_dir": f"{HDFS_ROOT}/mapreduce_output/feature_stats"
        },
        "degradation_metrics": {
            "script": "mr_degradation_metrics.py",
            "description": "Compute engine degradation metrics",
            "output_dir": f"{HDFS_ROOT}/mapreduce_output/degradation"
        },
        "sensor_stats": {
            "script": "mr_sensor_stats.py",
            "description": "Calculate sensor statistics (existing)",
            "output_dir": f"{HDFS_ROOT}/mapreduce_output/sensor_stats"
        },
        "op_count": {
            "script": "mr_op_count.py",
            "description": "Count records per unit (existing)",
            "output_dir": f"{HDFS_ROOT}/mapreduce_output/op_counts"
        },
        "rul_avg": {
            "script": "mr_rul_avg.py",
            "description": "Average RUL by operational setting (existing)",
            "output_dir": f"{HDFS_ROOT}/mapreduce_output/rul_averages"
        }
    },
    "runners": ["inline", "local", "hadoop"],
    "default_runner": "inline"
}

# ==================== HIVE CONFIGURATION ====================
HIVE_DATABASE = "cmapss_db"
HIVE_TABLES = {
    "train": "cmapss_train",
    "test": "cmapss_test",
    "rul": "cmapss_rul"
}

# ==================== MACHINE LEARNING CONFIGURATION ====================
ML_CONFIG = {
    "models_dir": os.path.join(BASE_DIR, "models"),
    "random_state": 42,
    "test_size": 0.2,
    "cv_folds": 5,
    "feature_columns": ["op_setting_1", "op_setting_2", "op_setting_3"] + 
                       [f"sensor_{i}" for i in range(1, 22)],
    "algorithms": {
        "random_forest": {
            "n_estimators": 100,
            "max_depth": 20,
            "min_samples_split": 5,
            "n_jobs": -1
        },
        "gradient_boosting": {
            "n_estimators": 100,
            "learning_rate": 0.1,
            "max_depth": 5
        }
    }
}

# ==================== COMMAND BUILDERS ====================
def get_hdfs_cmd_prefix():
    """Build HDFS command prefix based on Docker or native installation"""
    if USE_DOCKER:
        return ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs"]
    return ["hdfs", "dfs"]

def get_hive_cmd_prefix():
    """Build Hive command prefix based on Docker or native installation"""
    if USE_DOCKER:
        return ["docker", "exec", HIVE_SERVER_CONTAINER, "hive", "-e"] 
    return ["hive", "-e"]

def get_hadoop_cmd_prefix():
    """Build Hadoop command prefix for MapReduce jobs"""
    if USE_DOCKER:
        return ["docker", "exec", NAMENODE_CONTAINER, "hadoop"]
    return ["hadoop"]

def get_column_names():
    """Get list of column names from schema"""
    return CMAPSS_SCHEMA["columns"]

def get_dataset_info(dataset_id):
    """Get metadata for a specific dataset"""
    return DATASET_METADATA.get(dataset_id, {})

def get_all_datasets():
    """Get list of all dataset IDs"""
    return list(DATASET_METADATA.keys())

# ==================== VALIDATION HELPERS ====================
def validate_dataset_id(dataset_id):
    """Validate if dataset ID exists"""
    return dataset_id in DATASET_METADATA

def get_expected_columns_count():
    """Get expected number of columns in raw data"""
    return len(CMAPSS_SCHEMA["columns"])
