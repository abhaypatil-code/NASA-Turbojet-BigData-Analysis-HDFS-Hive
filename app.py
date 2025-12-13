"""
NASA Turbojet Predictive Maintenance Platform
Enhanced Streamlit UI with comprehensive tab-based organization
Production-quality interface for C-MAPSS big data analytics
"""

import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import socket
import shutil

# Backend imports
from backend.mongo_manager import MongoManager
from backend.hdfs_manager import HDFSManager
from backend.mapreduce_manager import MapReduceManager
from backend.hive_manager import HiveManager
from backend.data_ingestion import DataIngestion
from backend.model_service import ModelService
from backend.config import (
    CMAPS_DIR, HDFS_ROOT, DATASET_METADATA, CRITICAL_SENSORS,
    get_all_datasets, MAPREDUCE_CONFIG, USE_DOCKER
)

# ====================PAGE CONFIGURATION ====================
st.set_page_config(
    page_title="NASA Turbojet Analytics Platform",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== CUSTOM STYLING ====================
st.markdown("""
<style>
    .main {background-color: #0e1117;}
    .stButton>button {
        width: 100%;border-radius: 8px;font-weight: 600;
        transition: all 0.3s ease;border: 2px solid transparent;
    }
    .stButton>button:hover {
        transform: translateY(-2px);box-shadow: 0 4px 12px rgba(0, 255, 255, 0.3);
        border-color: #00ffff;
    }
    .metric-card {
        padding: 1.5rem;background: linear-gradient(135deg, #1e3a8a 0%, #312e81 100%);
        border-radius: 12px;border: 1px solid rgba(255, 255, 255, 0.1);
        text-align: center;box-shadow: 0 8px 16px rgba(0, 0, 0, 0.3);
        transition: transform 0.3s ease;
    }
    .metric-card:hover {transform: translateY(-5px);box-shadow: 0 12px 24px rgba(0, 0, 0, 0.4);}
    .metric-card h3 {margin: 0;color: #ffffff;font-size: 1.8rem;font-weight: 700;}
    .metric-card p {margin: 0.5rem 0 0 0;color: #a0aec0;font-size: 0.9rem;text-transform: uppercase;letter-spacing: 1px;}
    .status-success {padding: 12px 16px;border-radius: 8px;margin: 8px 0;background: rgba(25, 135, 84, 0.15);border-left: 4px solid #198754;color: #198754;}
    .status-error {padding: 12px 16px;border-radius: 8px;margin: 8px 0;background: rgba(220, 53, 69, 0.15);border-left: 4px solid #dc3545;color: #dc3545;}
    .status-warning {padding: 12px 16px;border-radius: 8px;margin: 8px 0;background: rgba(255, 193, 7, 0.15);border-left: 4px solid #ffc107;color: #ffc107;}
    .status-info {padding: 12px 16px;border-radius: 8px;margin: 8px 0;background: rgba(13, 110, 253, 0.15);border-left: 4px solid #0d6efd;color: #0d6efd;}
    h1 {background: linear-gradient(90deg, #00ffff, #0080ff);-webkit-background-clip: text;-webkit-text-fill-color: transparent;font-weight: 800;}
    h2 {color: #00bfff;font-weight: 700;}
    h3 {color: #00ffff;font-weight: 600;}
</style>
""", unsafe_allow_html=True)

# ==================== SESSION STATE ====================
class SessionState:
    def __init__(self):
        self.mongo = MongoManager()
        self.hdfs = HDFSManager()
        self.mr = MapReduceManager()
        self.hive = HiveManager()
        self.ingestion = DataIngestion()
        self.model = ModelService()

if 'services' not in st.session_state:
    st.session_state.services = SessionState()
elif not hasattr(st.session_state.services.mongo, 'get_sample_documents'):
    # Force reload if old instance is cached
    st.session_state.services = SessionState()
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

svc = st.session_state.services

# ==================== UTILITY FUNCTIONS ====================
def show_status_box(message, status_type="info"):
    st.markdown(f'<div class="status-{status_type}">{message}</div>', unsafe_allow_html=True)

def check_mongo_status():
    try:
        success, msg = svc.mongo.test_connection()
        return success, msg
    except:
        return False, "Connection error"

def check_hdfs_status():
    try:
        # Simple list of root to check connectivity
        files = svc.hdfs.list_files("/")
        return len(files) >= 0, "Active"
    except Exception as e:
        return False, f"Unreachable: {str(e)}"

def check_socket(host, port, timeout=2):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

# ==================== SIDEBAR ====================
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/e/e5/NASA_logo.svg", width=150)
    st.title("System Control Panel")
    
    if st.button("🔄 Refresh Status", use_container_width=True):
        st.session_state.last_refresh = datetime.now()
        st.rerun()
    
    st.markdown("---")
    
    st.subheader("🍃 MongoDB")
    mongo_ok, mongo_msg = check_mongo_status()
    if mongo_ok:
        st.success(f"✓ Connected")
        st.caption(f"_{mongo_msg}_")
    else:
        st.error(f"✗ Disconnected")
        st.caption(f"_{mongo_msg}_")
        if st.button("Run: docker start mongodb"):
            os.system("docker start mongodb")
            st.rerun()
    
    st.subheader("🐘 HDFS")
    hdfs_ok, hdfs_msg = check_hdfs_status()
    if hdfs_ok:
        st.success(f"✓ {hdfs_msg}")
    else:
        st.error(f"✗ {hdfs_msg}")
        if st.button("Run: docker start namenode"):
            os.system("docker start namenode datanode")
            st.rerun()
    
    st.markdown("---")
    
    st.subheader("📊 Quick Stats")
    if mongo_ok:
        summary = svc.mongo.get_summary()
        st.metric("Total Records", f"{summary.get('total_records', 0):,}")
        st.metric("Engine Units", summary.get('total_units', 0))
        st.metric("Datasets", len(summary.get('dataset_ids', [])))
    else:
        st.info("Connect MongoDB for stats")
    
    st.markdown("---")
    st.caption(f"Last refresh: {st.session_state.last_refresh.strftime('%H:%M:%S')}")

# ==================== MAIN TABS ====================
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "🏠 Home", "📤 Data Ingestion", "🔍 Data Exploration", "🍃 MongoDB Analytics",
    "🐘 HDFS Management", "🐝 HiveQL Queries", "⚡ MapReduce Jobs", "🤖 RUL Prediction"
])

# ==================== TAB 1: HOME ====================
with tab1:
    st.title("✈️ NASA Turbojet Predictive Maintenance Platform")
    st.markdown("### Big Data Analytics for C-MAPSS Turbofan Engine Degradation")
    
    # Startup Environment Check
    with st.expander("🛠️ Environment Status Check", expanded=not (mongo_ok and hdfs_ok)):
        chk_col1, chk_col2, chk_col3 = st.columns(3)
        chk_col1.metric("MongoDB Port (27017)", "OPEN" if check_socket("localhost", 27017) else "CLOSED")
        chk_col2.metric("HDFS WebUI (9870)", "OPEN" if check_socket("localhost", 9870) else "CLOSED")
        chk_col3.metric("Hive Server (10000)", "OPEN" if check_socket("localhost", 10000) else "CLOSED")
        
        if not (mongo_ok and hdfs_ok):
             st.warning("⚠️ Some services appear to be down. Please ensure Docker containers are running.")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown('<div class="metric-card"><h3>MongoDB</h3><p>Big Data Storage</p></div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="metric-card"><h3>HDFS + Hive</h3><p>Data Warehousing</p></div>', unsafe_allow_html=True)
    with col3:
        st.markdown('<div class="metric-card"><h3>MapReduce</h3><p>Distributed Processing</p></div>', unsafe_allow_html=True)
    with col4:
        st.markdown('<div class="metric-card"><h3>ML Models</h3><p>RUL Prediction</p></div>', unsafe_allow_html=True)
    
    st.markdown("---")
    st.subheader("📊 CMAPSS Dataset Overview")
    
    dataset_info = []
    for ds_id, metadata in DATASET_METADATA.items():
        dataset_info.append({
            "Dataset": ds_id,
            "Train Engines": metadata['train_engines'],
            "Test Engines": metadata['test_engines'],
            "Operating Conditions": metadata['operating_conditions'],
            "Fault Modes": metadata['fault_modes'],
            "Description": metadata['description']
        })
    
    df_datasets = pd.DataFrame(dataset_info)
    st.dataframe(df_datasets, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    col_guide1, col_guide2 = st.columns(2)
    with col_guide1:
        st.info("""**🚀 Getting Started**\n\n1. Navigate to **Data Ingestion** to upload CMAPSS datasets\n2. Use **Data Exploration** to visualize sensor trends\n3. Query data with **HiveQL Queries** (20 pre-built queries)\n4. Run **MapReduce Jobs** for distributed analytics\n5. Train models and predict RUL in **RUL Prediction**""")
    with col_guide2:
        st.warning("""**⚙️ System Requirements**\n\n- **MongoDB**: Required for analytics and storage\n- **Docker**: HDFS + Hive containers must be running\n- **Datasets**: Place CMAPSS files in `/CMAPSS` directory\n- **Resources**: 8GB RAM recommended for processing""")
    
    st.subheader("🔧 Turbofan Engine Architecture")
    st.image("turbofan_architecture.png", 
             caption="Detail of Turbofan Engine Components and Sensor Locations", use_container_width=True)

# ==================== TAB 2: DATA INGESTION ====================
with tab2:
    st.header("📤 Data Upload & Ingestion Pipeline")
    st.markdown("Upload and process CMAPSS datasets into MongoDB and HDFS for analysis.")
    
    col_upload, col_status = st.columns([2, 1])
    
    with col_upload:
        st.subheader("🚀 Ingestion Controls")
        selected_datasets = st.multiselect("Select datasets to ingest", options=get_all_datasets(), default=get_all_datasets())
        
        st.markdown("**Ingestion targets:**")
        st.markdown("- 🍃 **MongoDB**: `BDA_Project.sensors` collection")
        st.markdown("- 🐘 **HDFS**: `/bda_project/processed/` directory structure")
        
        if st.button("▶️ Run Full Ingestion Pipeline", type="primary", use_container_width=True):
            with st.status("Running ingestion pipeline...", expanded=True) as status:
                st.write("🔧 Initializing directories...")
                try:
                    st.write("📂 Processing files...")
                    report = svc.ingestion.process_and_upload()
                    st.write("✅ Processing complete!")
                    status.update(label="Ingestion Complete!", state="complete", expanded=False)
                    with st.expander("📋 View Detailed Report", expanded=True):
                        st.code(report)
                    show_status_box("Data ingestion successful! Data is now available in MongoDB and HDFS.", "success")
                except Exception as e:
                    status.update(label="Ingestion Failed", state="error")
                    show_status_box(f"Error during ingestion: {str(e)}", "error")
    
    with col_status:
        st.subheader("📊 Storage Summary")
        if mongo_ok:
            summary = svc.mongo.get_summary()
            st.metric("Total Records", f"{summary.get('total_records', 0):,}")
            st.metric("Engine Units", summary.get('total_units', 0))
            if 'datasets' in summary:
                st.markdown("**Per-Dataset Breakdown:**")
                for ds_id, ds_data in summary['datasets'].items():
                    with st.expander(f"📁 {ds_id}"):
                        st.write(f"Train: {ds_data.get('train_count', 0):,} records")
                        st.write(f"Test: {ds_data.get('test_count', 0):,} records")
        else:
            st.warning("MongoDB not connected")
        
        if hdfs_ok:
            st.markdown("**HDFS Storage:**")
            storage = svc.hdfs.get_storage_summary()
            total_mb = storage.get('total', 0) / (1024 * 1024)
            st.metric("Total Size", f"{total_mb:.2f} MB")
            st.metric("Train Data", f"{storage.get('train', 0) / (1024 * 1024):.2f} MB")

# ==================== TAB 3: DATA EXPLORATION ====================
with tab3:
    st.header("🔍 Data Exploration & Visualization")
    
    dataset_id = st.selectbox("Select Dataset", get_all_datasets(), key="explore_dataset")
    dataset_meta = DATASET_METADATA.get(dataset_id, {})
    
    col_meta1, col_meta2, col_meta3 = st.columns(3)
    col_meta1.metric("Operating Conditions", dataset_meta.get('operating_conditions', 'N/A'))
    col_meta2.metric("Fault Modes", dataset_meta.get('fault_modes', 'N/A'))
    col_meta3.metric("Train Engines", dataset_meta.get('train_engines', 'N/A'))
    
    st.info(f"**Description:** {dataset_meta.get('description', 'No description available')}")
    
    st.markdown("---")
    
    if mongo_ok:
        tab_viz, tab_stats, tab_unit = st.tabs(["📈 Sensor Trends", "📊 Statistics", "🔬 Unit Deep Dive"])
        
        with tab_viz:
            st.subheader("Sensor Trend Visualization")
            avg_data = svc.mongo.get_avg_sensors_per_unit(dataset_id, "train")
            
            if avg_data:
                df_avg = pd.DataFrame(avg_data)
                available_sensors = [col for col in df_avg.columns if col.startswith('avg_sensor_')]
                selected_sensors = st.multiselect("Select sensors", available_sensors, default=available_sensors[:4])
                
                if len(selected_sensors) > 0:
                    fig = px.scatter(df_avg, x='unit_number', y=selected_sensors,
                                    title=f"Average Sensor Values per Engine ({dataset_id})", height=500)
                    fig.update_layout(template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    if len(selected_sensors) > 1:
                        st.subheader("Sensor Correlation Heatmap")
                        corr_matrix = df_avg[selected_sensors].corr()
                        fig_corr = px.imshow(corr_matrix, title="Correlation Matrix", color_continuous_scale="RdBu_r")
                        fig_corr.update_layout(template="plotly_dark")
                        st.plotly_chart(fig_corr, use_container_width=True)
            else:
                st.warning("No data available. Run ingestion first.")
        
        with tab_stats:
            st.subheader("Statistical Summary")
            stats = svc.mongo.get_sensor_statistics(dataset_id, "train", CRITICAL_SENSORS)
            
            if stats:
                stats_data = [{
                    "Sensor": s,
                    "Min": round(stats.get(f"{s}_min", 0), 2),
                    "Max": round(stats.get(f"{s}_max", 0), 2),
                    "Mean": round(stats.get(f"{s}_avg", 0), 2),
                    "Std Dev": round(stats.get(f"{s}_std", 0), 2)
                } for s in CRITICAL_SENSORS]
                
                df_stats = pd.DataFrame(stats_data)
                st.dataframe(df_stats, use_container_width=True, hide_index=True)
                
                fig = go.Figure()
                fig.add_trace(go.Bar(name='Min', x=df_stats['Sensor'], y=df_stats['Min']))
                fig.add_trace(go.Bar(name='Max', x=df_stats['Sensor'], y=df_stats['Max']))
                fig.add_trace(go.Bar(name='Mean', x=df_stats['Sensor'], y=df_stats['Mean']))
                fig.update_layout(title="Sensor Statistics Comparison", barmode='group', template="plotly_dark", height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with tab_unit:
            st.subheader("🔬 Individual Engine Unit Analysis")
            unit_id = st.number_input("Enter Unit ID", min_value=1, max_value=300, value=1)
            
            if st.button("Load Unit Data"):
                trends = svc.mongo.get_sensor_trends(unit_id, dataset_id, "train")
                if trends:
                    df_trends = pd.DataFrame(trends)
                    st.success(f"Loaded {len(df_trends)} cycles for Unit {unit_id}")
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Cycles", df_trends['time_cycles'].max())
                    col2.metric("Avg HPC Temp", round(df_trends['sensor_3'].mean(), 2))
                    col3.metric("Avg HPC Pressure", round(df_trends['sensor_11'].mean(), 2))
                    
                    sensors_to_plot = st.multiselect("Select sensors", [f"sensor_{i}" for i in range(1, 22)], 
                                                    default=["sensor_2", "sensor_3", "sensor_11"])
                    
                    if sensors_to_plot:
                        fig = px.line(df_trends, x='time_cycles', y=sensors_to_plot,
                                    title=f"Sensor Trends for Unit {unit_id}", height=500)
                        fig.update_layout(template="plotly_dark")
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"No data found for Unit {unit_id}")
    else:
        st.error("MongoDB connection required for exploration")

# ==================== TAB 4: MONGODB ANALYTICS ====================
with tab4:
    st.header("🍃 MongoDB Advanced Analytics")
    
    if mongo_ok:
        dataset_id_mongo = st.selectbox("Select Dataset", get_all_datasets(), key="mongo_dataset")
        
        analytics_tab1, analytics_tab2, analytics_tab3, analytics_tab4 = st.tabs([
            "🏥 Health Scores", "📉 Degradation Analysis", "🎯 Condition-Based Metrics", "📄 Sample Documents"
        ])
        
        with analytics_tab1:
            st.subheader("Engine Health Scores")
            health_data = svc.mongo.get_unit_health_scores(dataset_id_mongo, "train")
            
            if health_data:
                df_health = pd.DataFrame(health_data)
                st.dataframe(df_health.head(20), use_container_width=True)
                
                fig = px.scatter(df_health, x='unit_number', y='health_index', size='max_life',
                                color='avg_pressure_hpc', title="Engine Health Index",
                                labels={'health_index': 'Health Index'}, height=500)
                fig.update_layout(template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No health data available")
        
        with analytics_tab2:
            st.subheader("Degradation Trend Analysis")
            st.markdown("Analyze how average sensor values change over operational cycles across the entire fleet. This helps identify degradation patterns common to all engines.")
            
            # Get data
            degradation_data = svc.mongo.get_degradation_trends(dataset_id_mongo, "train")
            
            if degradation_data:
                df_deg = pd.DataFrame(degradation_data)
                
                # Sensor selection
                available_sensors = [col for col in df_deg.columns if col.startswith("sensor_")]
                default_sensors = ["sensor_2", "sensor_3", "sensor_4", "sensor_7"] # Standard critical set
                # Filter defaults to only those that exist
                default_sensors = [s for s in default_sensors if s in available_sensors]
                
                selected_sensors = st.multiselect("Select Sensors to Visualize", 
                                                available_sensors, 
                                                default=default_sensors)
                
                if selected_sensors:
                    fig = px.line(df_deg, x='time_cycles', y=selected_sensors,
                                title=f"Average Sensor Degradation ({dataset_id_mongo})",
                                labels={'time_cycles': 'Operational Cycle', 'value': 'Sensor Reading'})
                    fig.update_layout(template="plotly_dark", hovermode="x unified")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.caption(f"Based on {df_deg['unit_count'].iloc[0]} engine units")
                else:
                    st.info("Select sensors to visualize trends")
            else:
                st.warning("No degradation data available")
        
        with analytics_tab3:
            st.subheader("Condition-Based Performance Metrics")
            if dataset_id_mongo in ["FD002", "FD004"]:
                conditions = svc.mongo.get_condition_based_metrics(dataset_id_mongo, "train")
                if conditions:
                    df_cond = pd.DataFrame(conditions)
                    st.dataframe(df_cond, use_container_width=True)
                    
                    fig = px.bar(df_cond.head(15), x='operational_condition', y='unique_engines',
                                title="Engine Distribution by Operating Condition")
                    fig.update_layout(template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"{dataset_id_mongo} has single operating condition. Use FD002 or FD004 for multi-condition analysis.")
        
        with analytics_tab4:
            st.subheader("📄 Raw Document Inspection")
            st.markdown("View a sample of raw documents stored in MongoDB to understand the data structure.")
            
            sample_limit = st.slider("Number of documents", 1, 20, 5)
            
            samples = svc.mongo.get_sample_documents(dataset_id_mongo, "train", limit=sample_limit)
            
            if samples:
                st.write(f"Showing {len(samples)} documents from `{dataset_id_mongo}`:")
                st.json(samples)
                
                with st.expander("View as DataFrame"):
                    st.dataframe(pd.DataFrame(samples), use_container_width=True)
            else:
                st.warning("No documents found. Please run data ingestion first.")
    else:
        st.error("MongoDB connection required")

# ==================== TAB 5: HDFS MANAGEMENT ====================
# ==================== TAB 5: HDFS MANAGEMENT ====================
with tab5:
    st.header("🐘 HDFS File Management")
    
    # Strictly structured tabs as requested
    hdfs_nav = st.radio("Select Operation", 
        ["🟢 Install Hadoop", "➕ Adding files and directories", "🔍 Retrieving files", "🗑️ Deleting files and directories"],
        horizontal=True, label_visibility="collapsed")
    
    st.markdown("---")

    # 1. INSTALL HADOOP
    if "Install" in hdfs_nav:
        st.subheader("🟢 Install & Configure Hadoop")
        
        col_status, col_guide = st.columns(2)
        
        with col_status:
            st.markdown("### Current Status")
            if hdfs_ok:
                st.success("✅ **Hadoop is Installed and Running**")
                st.info(f"**Connection:** Active\n\n**WebUI:** http://localhost:9870")
            else:
                st.error("❌ **Hadoop Connection Failed**")
                st.warning("Hadoop services are not reachable.")
        
        with col_guide:
            st.markdown("### Installation Guide")
            st.markdown("""
            **Requirement:** This system uses **Docker** to run Hadoop.
            
            **Steps to Install/Run:**
            1.  Ensure Docker Desktop is installed.
            2.  Open Terminal in project root.
            3.  Run: `docker-compose up -d`
            4.  Wait for containers (`namenode`, `datanode`) to start.
            """)
            
            if not hdfs_ok:
                if st.button("🚀 Attempt to Start Hadoop (Docker)"):
                    with st.spinner("Starting containers..."):
                        os.system("docker start namenode datanode hive-server")
                        st.rerun()

    # 2. ADDING FILES AND DIRECTORIES
    elif "Adding" in hdfs_nav:
        st.subheader("➕ Adding Files and Directories")
        
        if not hdfs_ok:
            st.error("Please connect to Hadoop first (See 'Install Hadoop' tab).")
        else:
            col_add1, col_add2 = st.columns(2)
            
            with col_add1:
                st.markdown("#### 📁 Create Directory")
                new_dir_path = st.text_input("Parent Path", value=HDFS_ROOT, key="mkdir_parent")
                new_dir_name = st.text_input("New Directory Name", key="mkdir_name")
                
                if st.button("Create Directory"):
                    if new_dir_name:
                        full_path = f"{new_dir_path}/{new_dir_name}".replace("//", "/")
                        success, msg = svc.hdfs.mkdir(full_path)
                        if success:
                            show_status_box(f"Created: `{full_path}`", "success")
                        else:
                            show_status_box(f"Failed: {msg}", "error")
                    else:
                        st.warning("Enter directory name")
            
            with col_add2:
                st.markdown("#### ⬆️ Upload File")
                dest_path = st.text_input("Destination Path", value=f"{HDFS_ROOT}/uploads/", key="upload_dest")
                uploaded_file = st.file_uploader("Select File")
                
                if uploaded_file and st.button("Upload File"):
                    temp_path = f"temp_{uploaded_file.name}"
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    svc.hdfs.mkdir(dest_path) # Ensure dest exists
                    final_dest = f"{dest_path}/{uploaded_file.name}".replace("//", "/")
                    
                    success, msg = svc.hdfs.upload_file(temp_path, final_dest)
                    
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    
                    if success:
                        show_status_box(f"Uploaded: `{final_dest}`", "success")
                    else:
                        show_status_box(f"Failed: {msg}", "error")

    # 3. RETRIEVING FILES
    elif "Retrieving" in hdfs_nav:
        st.subheader("🔍 Retrieving Files")
        
        if not hdfs_ok:
            st.error("Please connect to Hadoop first (See 'Install Hadoop' tab).")
        else:
            path_browse = st.text_input("Browse Path", value=HDFS_ROOT, key="retrieve_path")
            
            files = svc.hdfs.list_files(path_browse)
            if files:
                # Filter only files for clear retrieval
                file_list = [f for f in files if f['type'] == 'file']
                
                if file_list:
                    st.markdown(f"**Found {len(file_list)} files in `{path_browse}`:**")
                    
                    # Create a readable table for selection
                    df_files = pd.DataFrame(file_list)[['name', 'size', 'date', 'owner']]
                    st.dataframe(df_files, use_container_width=True)
                    
                    st.markdown("### Download Selection")
                    selected_file_name = st.selectbox("Select File to Retrieve", [f['name'] for f in file_list])
                    
                    if selected_file_name:
                        col_ret1, col_ret2 = st.columns(2)
                        full_hdfs_path = f"{path_browse}/{selected_file_name}".replace("//", "/")
                        
                        with col_ret1:
                            if st.button("⬇️ Retrieve (Download)"):
                                local_dest = f"downloads/{selected_file_name}"
                                success, msg = svc.hdfs.download_file(full_hdfs_path, local_dest)
                                if success:
                                    st.success(f"File Retrieved: `{local_dest}`")
                                    with open(local_dest, "rb") as f:
                                        st.download_button("Save to Local Disk", f, file_name=selected_file_name)
                                else:
                                    st.error(f"Retrieval Failed: {msg}")
                        
                        with col_ret2:
                            if st.button("👁️ Retrieve Content (Preview)"):
                                success, content = svc.hdfs.cat_file(full_hdfs_path, head_bytes=2000)
                                if success:
                                    st.markdown("**File Content Preview:**")
                                    st.code(content)
                                else:
                                    st.error(f"Read Failed: {content}")
                else:
                    st.info("No files found in this directory (only directories).")
            else:
                st.warning("Path not found or empty.")

    # 4. DELETING FILES AND DIRECTORIES
    elif "Deleting" in hdfs_nav:
        st.subheader("🗑️ Deleting Files and Directories")
        
        if not hdfs_ok:
            st.error("Please connect to Hadoop first (See 'Install Hadoop' tab).")
        else:
            st.warning("⚠️ Warning: Deletion is permanent.")
            
            del_path = st.text_input("Full Path to Delete", value=f"{HDFS_ROOT}/uploads/filename.txt", key="del_path")
            
            col_del1, col_del2 = st.columns([1, 4])
            with col_del1:
                # Double confirmation UI
                if st.button("❌ DELETE"):
                    if del_path == "/" or del_path == HDFS_ROOT:
                         st.error("Safety Block: Cannot delete root directories via UI.")
                    else:
                         success, msg = svc.hdfs.delete_file(del_path)
                         if success:
                             show_status_box(f"Successfully Deleted: `{del_path}`", "success")
                         else:
                             show_status_box(f"Deletion Failed: {msg}", "error")
            
            st.markdown("---")
            st.markdown("### Browse to find paths")
            # Mini browser helper
            browse_path = st.text_input("Helper Browser", value=HDFS_ROOT, key="del_browse_path")
            files = svc.hdfs.list_files(browse_path)
            if files:
                 st.dataframe(pd.DataFrame(files)[['name', 'type', 'size']], use_container_width=True)

# ==================== TAB 6: HIVEQL QUERIES ====================
with tab6:
    st.header("🐝 HiveQL Query Interface")
    
    query_tab1, query_tab2 = st.tabs(["📚 Pre-built Queries (20)", "✏️ Custom Query"])
    
    with query_tab1:
        st.subheader("20 Pre-built HiveQL Demonstration Queries")
        
        # Initialize Hive tables button
        if st.button("🔧 Initialize Hive Tables", use_container_width=True):
            with st.spinner("Creating Hive tables..."):
                results = svc.hive.create_cmapss_tables()
                for success, msg in results:
                    if success:
                        show_status_box(msg, "success")
                    else:
                        show_status_box(msg, "error")
        
        st.markdown("---")
        
        # Get all queries
        queries = svc.hive.get_prebuilt_queries()
        
        # Category filter
        categories = list(set([q['category'] for q in queries.values()]))
        selected_category = st.selectbox("Filter by category", ["All"] + sorted(categories))
        
        # Display queries
        for qid, qinfo in queries.items():
            if selected_category == "All" or qinfo['category'] == selected_category:
                with st.expander(f"**{qinfo['name']}** ({qinfo['category']})"):
                    st.markdown(f"*{qinfo['description']}*")
                    st.code(qinfo['query'], language='sql')
                    
                    if st.button(f"▶️ Execute {qid}", key=f"exec_{qid}"):
                        with st.spinner(f"Executing {qinfo['name']}..."):
                            success, result, metadata = svc.hive.execute_prebuilt_query(qid)
                            
                            if success:
                                st.success(f"Query executed successfully!")
                                with st.expander("📊 Results", expanded=True):
                                    st.code(result)
                            else:
                                st.error(f"Query failed: {result}")
    
    with query_tab2:
        st.subheader("Custom HiveQL Query")
        
        custom_query = st.text_area("Enter your HiveQL query:", 
                                    value="SELECT * FROM cmapss_train LIMIT 10",
                                    height=150)
        
        if st.button("🚀 Execute Custom Query", type="primary"):
            with st.spinner("Executing query..."):
                success, result = svc.hive.run_query(custom_query)
                
                if success:
                    st.success("Query executed!")
                    st.code(result)
                else:
                    st.error(f"Query failed: {result}")

# ==================== TAB 7: MAPREDUCE JOBS ====================
with tab7:
    st.header("⚡ MapReduce Distributed Processing")
    
    st.markdown("Run MapReduce jobs for large-scale data processing.")
    
    # Job selection
    job_configs = MAPREDUCE_CONFIG['jobs']
    job_names = list(job_configs.keys())
    
    selected_job_key = st.selectbox("Select MapReduce Job", job_names,
                                    format_func=lambda x: f"{x}: {job_configs[x]['description']}")
    
    job_info = job_configs[selected_job_key]
    
    st.info(f"**Job Description:** {job_info['description']}")
    st.caption(f"Output Directory: `{job_info['output_dir']}`")
    
    # YARN Execution Info
    st.success("🐳 Jobs will run on Hadoop YARN cluster via Docker NameNode container")
    
    # Input file selection (HDFS only)
    input_file = st.text_input("Input File (HDFS Path)", value="/bda_project/processed/train/FD001.csv",
                               help="Specify the full HDFS path to your input data file")
    
    if st.button("▶️ Run MapReduce Job on YARN Cluster", type="primary", use_container_width=True):
        with st.status(f"Running {job_info['script']} on Hadoop YARN...", expanded=True) as status:
            st.write("🚀 Initializing job on YARN cluster...")
            script_name = job_info['script']
            success, output = svc.mr.run_job(script_name, input_file, runner="hadoop")
            
            if success:
                status.update(label="Job Completed Successfully!", state="complete", expanded=False)
                st.success("✅ MapReduce job completed successfully!")
                
                st.subheader("📊 Job Results")
                
                # Parse and display output in a beautiful format
                try:
                    # Parse the tab-separated output with JSON values
                    parsed_results = []
                    for line in output.strip().split('\n'):
                        if line.strip():
                            parts = line.split('\t')
                            if len(parts) >= 2:
                                key = parts[0].strip().strip('"')
                                try:
                                    value = json.loads(parts[1])
                                    if isinstance(value, dict):
                                        # Only include results with avg, min, max, count (sensor stats format)
                                        if all(k in value for k in ['avg', 'min', 'max', 'count']):
                                            parsed_results.append({"Key": key, **value})
                                except:
                                    pass  # Skip non-JSON or incomplete entries
                    
                    if parsed_results:
                        df_results = pd.DataFrame(parsed_results)
                        
                        # Display summary metrics
                        st.markdown("### 📈 Summary Metrics")
                        metric_cols = st.columns(min(4, len(parsed_results)))
                        for idx, row in enumerate(parsed_results[:4]):
                            with metric_cols[idx % 4]:
                                key_name = row.get("Key", f"Item {idx+1}")
                                if "count" in row:
                                    st.metric(key_name, f"{row['count']:,} records")
                                elif "avg" in row:
                                    st.metric(key_name, f"Avg: {row['avg']:.2f}")
                                elif "Value" in row:
                                    st.metric(key_name, row["Value"])
                        
                        st.markdown("---")
                        
                        # Display styled data table
                        st.markdown("### 📋 Detailed Results")
                        
                        # Style the dataframe
                        if "min" in df_results.columns and "max" in df_results.columns:
                            # Sensor stats format - show as styled table
                            display_df = df_results.copy()
                            for col in ["min", "max", "avg"]:
                                if col in display_df.columns:
                                    display_df[col] = display_df[col].apply(lambda x: f"{x:.4f}" if isinstance(x, (int, float)) else x)
                            if "count" in display_df.columns:
                                display_df["count"] = display_df["count"].apply(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
                            
                            st.dataframe(
                                display_df,
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    "Key": st.column_config.TextColumn("📌 Sensor/Feature", width="medium"),
                                    "min": st.column_config.TextColumn("⬇️ Min", width="small"),
                                    "max": st.column_config.TextColumn("⬆️ Max", width="small"),
                                    "avg": st.column_config.TextColumn("📊 Average", width="small"),
                                    "count": st.column_config.TextColumn("🔢 Count", width="small"),
                                }
                            )
                            
                            # Create visualization
                            st.markdown("### 📊 Visual Analysis")
                            if "avg" in df_results.columns:
                                fig = px.bar(
                                    df_results.head(15), 
                                    x="Key", 
                                    y="avg",
                                    title="Average Values by Feature",
                                    color="avg",
                                    color_continuous_scale="Viridis"
                                )
                                fig.update_layout(
                                    template="plotly_dark",
                                    xaxis_tickangle=-45,
                                    height=400
                                )
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Min/Max range chart
                                if "min" in df_results.columns and "max" in df_results.columns:
                                    fig2 = go.Figure()
                                    fig2.add_trace(go.Bar(
                                        name='Min', 
                                        x=df_results['Key'].head(15), 
                                        y=df_results['min'].head(15),
                                        marker_color='#00bcd4'
                                    ))
                                    fig2.add_trace(go.Bar(
                                        name='Max', 
                                        x=df_results['Key'].head(15), 
                                        y=df_results['max'].head(15),
                                        marker_color='#ff5722'
                                    ))
                                    fig2.update_layout(
                                        title="Min/Max Range Comparison",
                                        barmode='group',
                                        template="plotly_dark",
                                        xaxis_tickangle=-45,
                                        height=400
                                    )
                                    st.plotly_chart(fig2, use_container_width=True)
                        else:
                            # Generic format
                            st.dataframe(df_results, use_container_width=True, hide_index=True)
                        
                        # Show record count
                        st.markdown(f"**Total Results:** {len(parsed_results)} items processed")
                    else:
                        st.info("No structured data to display")
                        st.code(output, language="text")
                        
                except Exception as parse_error:
                    st.warning(f"Could not parse structured output: {parse_error}")
                    st.code(output, language="text")
                
                # Expandable raw output
                with st.expander("🔍 View Raw Output"):
                    st.code(output, language="text")
                    
            else:
                status.update(label="Job Failed", state="error", expanded=True)
                st.error("❌ Job failed")
                st.code(output, language="text")

# ==================== TAB 8: RUL PREDICTION ====================
with tab8:
    st.header("🤖 Remaining Useful Life (RUL) Prediction")
    
    dataset_id_pred = st.selectbox("Target Dataset", get_all_datasets(), key="pred_dataset")
    
    train_tab, predict_tab = st.tabs(["🎓 Model Training", "🔮 Prediction"])
    
    with train_tab:
        st.subheader("Train RUL Prediction Model")
        
        st.markdown(f"""
        **Training Configuration:**
        - Algorithm: Random Forest Regressor
        - Features: 3 operational settings + 21 sensors
        - Target: Remaining Useful Life (RUL)
        """)
        
        if st.button("🎓 Start Training", type="primary", use_container_width=True):
            with st.status(f"Training model for {dataset_id_pred}...", expanded=True) as status:
                st.write("📥 Loading training data...")
                st.write("⚙️ Engineering features...")
                st.write("🌲 Training Random Forest...")
                
                success, msg = svc.model.train_model(dataset_id_pred)
                
                if success:
                    st.write("💾 Saving model...")
                    status.update(label="Training Complete!", state="complete")
                    st.balloons()
                    show_status_box(msg, "success")
                else:
                    status.update(label="Training Failed", state="error")
                    show_status_box(msg, "error")
    
    with predict_tab:
        st.subheader("Predict RUL for Test Engines")
        
        unit_to_test = st.number_input("Enter Unit Number (Test Set)", min_value=1, value=1, key="pred_unit")
        
        if st.button("🔮 Predict Remaining Useful Life", type="primary"):
            with st.spinner("Running inference..."):
                res, msg = svc.model.predict_rul(dataset_id_pred, unit_to_test)
                
                if res is not None and not res.empty:
                    last_row = res.iloc[-1]
                    pred_rul = last_row['predicted_rul']
                    curr_cycle = last_row['time_cycles']
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Current Cycle", int(curr_cycle))
                    col2.metric("Predicted RUL", f"{pred_rul:.1f} cycles")
                    col3.metric("Total Predicted Life", f"{curr_cycle + pred_rul:.1f}")
                    
                    fig = px.line(res, x='time_cycles', y='predicted_rul',
                                title=f"RUL Trajectory for Unit {unit_to_test}",
                                labels={'predicted_rul': 'Predicted RUL', 'time_cycles': 'Operational Cycle'})
                    fig.update_layout(template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.caption("RUL trajectory shows predicted remaining life at each operational cycle")
                else:
                    show_status_box(f"Prediction failed: {msg}", "error")
