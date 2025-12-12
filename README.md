# Big Data Analytics Project - Preventative Maintenance (NASA CMaps)

This project is a complete Big Data pipeline implementation for the NASA CMaps Turbofan Engine Degradation dataset. It integrates MongoDB, Hadoop (HDFS & MapReduce), and Hive into a unified Streamlit dashboard.

## 📌 Project Overview
- **Data Source**: NASA CMaps (Turbofan Engine Degradation Simulation)
- **Tech Stack**:
  - **Frontend**: Streamlit
  - **Database**: MongoDB (NoSQL)
  - **Big Data Storage**: HDFS (Hadoop Distributed File System)
  - **Processing**: MapReduce (Python mrjob)
  - **Warehousing**: Hive (SQL-like querying)

## 📂 Directory Structure
```
BDA_Project/
├── CMaps/                  # Dataset folder
├── backend/                # Backend logic managers
│   ├── config.py           # Configuration (Path, URIs)
│   ├── mongo_manager.py    # MongoDB operations
│   ├── hdfs_manager.py     # HDFS wrappers
│   ├── mapreduce_manager.py# Job submission logic
│   └── hive_manager.py     # HiveQL execution logic
├── mapreduce_jobs/         # MapReduce Python scripts
│   ├── mr_op_count.py      # Job: Count ops per unit
│   └── mr_sensor_stats.py  # Job: Sensor statistics
├── app.py                  # Main Streamlit Dashboard
└── requirements.txt        # Python dependencies
```

## 🚀 Setup Instructions

### 1. Prerequisites
- **Python 3.8+**
- **MongoDB**: Installed and running locally on default port (27017).
- **Hadoop**: Installed and configured (commands `hdfs`, `hadoop` must be in PATH).
- **Hive**: Installed and configured (command `hive` must be in PATH).
- *(Optional but Recommended)*: Docker environment for Hadoop/Hive if native Windows installation is not available.

### 2. Installation
1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   pip install setuptools  # Required for mrjob on Python 3.12+
   ```

2. Start MongoDB:
   - Ensure `mongod` is running.

3. Start Hadoop & Hive:
   - Ensure HDFS NameNode and DataNodes are running (`start-dfs.sh` or via Docker).
   - Ensure Hive Server is running.

### 3. Running the Application
Run the Streamlit app:
```bash
streamlit run app.py
```

## 🛠 Features & Usage

### 1. MongoDB Analytics
- Go to the **MongoDB** tab.
- **Ingest Data**: Select a CMaps file (e.g., `train_FD004.txt`) and click "Ingest". This loads data into MongoDB.
- **Dashboard**: View sensor correlations and unit trends.

### 2. Hadoop (HDFS & MapReduce)
- Go to **Hadoop & MapReduce** tab.
- **HDFS**: Upload files from local `CMaps` to HDFS. Manage files (List/Delete).
- **MapReduce**:
  - Select a job (e.g., Sensor Statistics).
  - Choose "Runner":
    - **Inline**: Runs locally (simulated) - requires no Hadoop cluster. Good for testing logic.
    - **Hadoop**: Submits actual job to the cluster - requires running Hadoop environment.

### 3. Hive Data Warehouse
- Go to **HiveSQL** tab.
- **Initialize**: Click "Initialize Table" to create the external table `cmaps_sensors` pointing to HDFS data.
- **Query**: Write and execute SQL queries (e.g., `SELECT unit_number, AVG(s11) FROM cmaps_sensors GROUP BY unit_number`).

## ⚠️ Troubleshooting
- **Missing 'pipes' module**: The code includes a shim for Python 3.13+. If issues persist, ensure `setuptools` is installed.
- **Hadoop not found**: If running on Windows without Hadoop, use the "Inline" runner for MapReduce and rely on MongoDB for analytics.
- **Connection Errors**: Check `backend/config.py` to adjust URIs or Docker settings if using containers.
