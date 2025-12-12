# System Setup Guide

It appears clearly that you are missing the required backend services (MongoDB, Hadoop, Hive). 
We recommend using **Docker** to run these services easily without complex Windows installation.

## Option 1: The Docker Way (Recommended)
This method installs all dependencies (MongoDB, Hadoop, Hive) in isolated containers.

### 1. Install Docker Desktop
*   Download and install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/).
*   Restart your computer if prompted.
*   Start Docker Desktop and wait for the engine to start.
*   Verify installation by running `docker --version` in your terminal.

### 2. Start Services
Open a terminal in this project folder (`D:\Software\Projects\NASATurbojet-BigDataAnalysis-using-HDFS-and-Hive`) and run:
```bash
docker-compose up -d
```
This will download and start:
*   MongoDB (Port 27017)
*   Hadoop NameNode & DataNode
*   Hive Server

### 3. Update Configuration
Open `backend/config.py` in your code editor and change:
```python
USE_DOCKER = True
```
This tells the application to communicate with the Docker containers instead of looking for local installations.

### 4. Restart the App
Stop the streamlit app (Ctrl+C) and run it again:
```bash
streamlit run app.py
```

---

## Option 2: Local Installation (Advanced)
If you cannot use Docker, you must install the services manually.

### 1. MongoDB
*   Download [MongoDB Community Server](https://www.mongodb.com/try/download/community).
*   Install it and ensure it is running as a Service.
*   Add `mongod` to your System PATH.

### 2. Hadoop & Hive
*   Installing Hadoop and Hive native on Windows is complex and error-prone.
*   **Workaround**: You can run the app with **MongoDB only**.
    *   Install MongoDB as above.
    *   The "Hadoop" and "Hive" checks will fail, but the "MongoDB Analytics" tab will work.
    *   For MapReduce, use the "Inline" runner option in the UI.
    *   Hive features will not work.
