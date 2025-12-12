# Big Data Analytics Project -  (NASA CMAPSS)

This project is a complete **Big Data Analytics Platform** for the NASA C-MAPSS Turbofan Engine Degradation dataset. It integrates **HDFS**, **Hive**, **Machine Learning (Scikit-Learn)**, and **Streamlit** into a unified dashboard for predictive maintenance.

## 📌 Project Overview
- **Data Source**: NASA CMaps (Turbofan Engine Degradation Simulation)
- **Goal**: Predict the **Remaining Useful Life (RUL)** of jet engines.
- **Tech Stack**:
  - **Frontend**: Streamlit (Data Management, Dashboard, Prediction Interface)
  - **Storage**: HDFS (Hadoop Distributed File System) via Docker
  - **Warehousing**: Hive (External Tables & SQL Querying)
  - **Inference**: Random Forest Regressor (Scikit-Learn)
  - **Processing**: Python-based Ingestion & MapReduce

## 📂 Directory Structure
```
BDA_Project/
├── CMAPSS/                 # Raw Dataset folder
├── backend/                # Core logic
│   ├── config.py           # Configuration
│   ├── data_ingestion.py   # Data cleaning & HDFS upload script
│   ├── hive_manager.py     # HiveQL execution & Table management
│   ├── modal_service.py    # ML Training & Prediction logic
│   ├── hdfs_manager.py     # HDFS CLI wrapper
│   └── mongo_manager.py    # MongoDB (Legacy/Alternative store)
├── models/                 # Serialized ML models (.pkl)
├── app.py                  # Main Streamlit Dashboard
└── requirements.txt        # Python dependencies
```

## 🚀 Setup Instructions

### 1. Prerequisites
- **Python 3.9+**
- **Docker Desktop** (Recommended for managing Hadoop/Hive containers).
- **Hadoop/Hive Containers**: Ensure your container names match `backend/config.py` (default: `namenode`, `hive-server`).

### 2. Installation
1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start your Docker Environment (HDFS & Hive).

### 3. Running the Application
Run the Streamlit app:
```bash
streamlit run app.py
```

## 🛠 Features & Usage

### 1. Data Pipeline Management
- **Ingestion**: Automatically scans `CMAPSS/` folder, cleans variable-space CSVs, adds `dataset_id`, and uploads to HDFS (`/bda_project/processed`).
- **Hive Integration**: Initializes external tables (`cmapss_train`, `cmapss_test`, `cmapss_rul`) mapped to HDFS directories.

### 2. Fleet Dashboard
- **Visualizations**: Interactive plots using Plotly.
- **Analysis**: Drill down into specific Engine Units to see sensor trends (e.g., Pressure, Temperature vs Cycles).

### 3. Predictive Maintenance (AI)
- **Training**: Train a **Random Forest Regressor** on the historical `train` dataset.
- **Inference**: Predict RUL for engines in the `test` dataset.
- **Metrics**: Compare predicted RUL vs Ground Truth (where available).

### 4. Hive Data Warehouse
- execute ad-hoc SQL queries like:
  ```sql
  SELECT unit_number, AVG(sensor_11) FROM cmapss_train GROUP BY unit_number
  ```

## ⚠️ Notes
- The project assumes a Dockerized Hadoop environment. If using a native installation, set `USE_DOCKER = False` in `backend/config.py`.
- First run requires clicking **"Run Full Ingestion Pipeline"** in the app to populate HDFS.
