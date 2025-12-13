# System Setup Guide

This guide provides detailed instructions for setting up the NASA Turbojet Predictive Maintenance Platform.

---

## Prerequisites

- **Docker Desktop** (required for Hadoop, Hive, and MongoDB)
- **Python 3.9+**
- **Git**
- **8GB RAM minimum** (16GB recommended)

---

## Quick Setup (Docker - Recommended)

This method installs all dependencies (MongoDB, Hadoop, Hive) in isolated containers.

### Step 1: Install Docker Desktop

1. Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Restart your computer if prompted
3. Start Docker Desktop and wait for the engine to start
4. Verify installation:
   ```bash
   docker --version
   docker-compose --version
   ```

### Step 2: Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/NASATurbojet-BigDataAnalysis-using-HDFS-and-Hive.git
cd NASATurbojet-BigDataAnalysis-using-HDFS-and-Hive
```

### Step 3: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Start Docker Services

```bash
docker-compose up -d
```

This will start the following services:
| Service | Port | Description |
|---------|------|-------------|
| MongoDB | 27017 | Document database |
| NameNode | 9870, 8088 | HDFS master + YARN |
| DataNode | 9864 | HDFS worker |
| Hive Server | 10000 | SQL over HDFS |

### Step 5: Verify Services

```bash
# Check all containers are running
docker ps

# Expected output should show:
# - namenode
# - datanode
# - hive-server
# - mongodb
```

### Step 6: Configure the Application

Edit `backend/config.py`:
```python
USE_DOCKER = True
NAMENODE_CONTAINER = "namenode"
```

### Step 7: Run the Application

```bash
streamlit run app.py
```

The application will open at `http://localhost:8501`

---

## Service Access URLs

| Service | URL | Description |
|---------|-----|-------------|
| **Streamlit App** | http://localhost:8501 | Main dashboard |
| **HDFS Web UI** | http://localhost:9870 | File browser |
| **YARN Resource Manager** | http://localhost:8088 | Job monitoring |
| **MongoDB** | mongodb://localhost:27017 | Database connection |

---

## First-Time Setup Steps

After starting the application:

1. **Verify Connections**: Check the sidebar shows all services connected (green status)

2. **Ingest Data**: Go to "Data Ingestion" tab and run the full ingestion pipeline

3. **Initialize Hive**: Go to "HiveQL Queries" tab and click "Initialize Hive Tables"

4. **Test MapReduce**: Go to "MapReduce Jobs" tab and run a sample job

5. **Train Model**: Go to "RUL Prediction" tab and train a model on FD001

---

## Troubleshooting

### Docker Issues

**Containers not starting:**
```bash
# Stop all containers
docker-compose down

# Remove volumes (fresh start)
docker-compose down -v

# Start again
docker-compose up -d
```

**Port conflicts:**
```bash
# Check if ports are in use
netstat -an | findstr :27017
netstat -an | findstr :9870

# Stop conflicting services or change ports in docker-compose.yml
```

### HDFS Issues

**Safe mode issues:**
```bash
# Exit safe mode
docker exec namenode hdfs dfsadmin -safemode leave
```

**Directory not found:**
```bash
# Create HDFS directories
docker exec namenode hdfs dfs -mkdir -p /bda_project/processed/train
docker exec namenode hdfs dfs -mkdir -p /bda_project/processed/test
docker exec namenode hdfs dfs -mkdir -p /bda_project/processed/rul
```

### MongoDB Issues

**Connection refused:**
```bash
# Check MongoDB is running
docker logs mongodb

# Restart MongoDB
docker restart mongodb
```

---

## Alternative: Local Installation (Advanced)

If you cannot use Docker, you can install services locally:

### MongoDB
1. Download [MongoDB Community Server](https://www.mongodb.com/try/download/community)
2. Install and start as a service
3. Verify: `mongosh` or `mongo`

### Hadoop & Hive
> ⚠️ Installing Hadoop and Hive natively on Windows is complex. We recommend using Docker.

If running without Hadoop/Hive:
- MongoDB features will work
- Use "Inline" execution mode for MapReduce
- Hive features will be unavailable

---

## System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disk | 20 GB | 50 GB |
| OS | Windows 10/11, Linux, macOS | |

---

## Need Help?

1. Check the [README.md](README.md) for project overview
2. Review [architecture.md](architecture.md) for system design
3. Check Docker logs: `docker logs <container-name>`
4. Open an issue on GitHub
