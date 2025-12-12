import streamlit as st
import pandas as pd
import os
import shutil
import plotly.express as px
from backend.mongo_manager import MongoManager
from backend.hdfs_manager import HDFSManager
from backend.mapreduce_manager import MapReduceManager
from backend.hive_manager import HiveManager
from backend.data_ingestion import DataIngestion
from backend.model_service import ModelService
from backend.config import CMAPS_DIR, HDFS_ROOT, CMAPSS_SCHEMA, USE_DOCKER

# Page Config
st.set_page_config(
    page_title="NASA Turbojet Analytics",
    page_icon="✈️",
    layout="wide",
)

# Custom Styling
st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        background-color: #007bff;
        color: white;
    }
    .metric-card {
        padding: 1rem;
        background-color: #f8f9fa;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

def check_services():
    with st.sidebar:
        st.header("System Status")
        col1, col2 = st.columns(2)
        
        # MongoDB Check
        mm = MongoManager()
        m_ok, _ = mm.test_connection()
        col1.write("🍃 MongoDB")
        col2.write("✅" if m_ok else "❌")
        
        # HDFS Check
        hm = HDFSManager()
        # Simple check by listing root
        h_ok = False
        try:
            if hm.list_files("/"): h_ok = True
        except: pass
        
        if USE_DOCKER and shutil.which("docker"):
             h_icon = "🐳"
        else:
             h_icon = "🐘"
             
        col1.write(f"{h_icon} HDFS")
        col2.write("✅" if h_ok else "⚠️")

def render_home():
    st.title("✈️ NASA Turbojet Predictive Maintenance")
    st.markdown("""
    ### Big Data Analytics Platform
    
    This application processes the **NASA C-MAPSS** Jet Engine Dataset to predict Remaining Useful Life (RUL).
    
    #### Architecture:
    *   **Ingestion**: Python -> HDFS (Dockerized)
    *   **Warehousing**: Hive (External Tables)
    *   **Processing**: MapReduce & Spark (Simulated/Integrated)
    *   **Analytics**: Random Forest Regression for RUL
    """)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown('<div class="metric-card"><h3>Data Pipeline</h3><p>Manage HDFS & Hive</p></div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="metric-card"><h3>Fleet Status</h3><p>Visualize Sensor Data</p></div>', unsafe_allow_html=True)
    with col3:
        st.markdown('<div class="metric-card"><h3>AI Prediction</h3><p>Predict Engine Failure</p></div>', unsafe_allow_html=True)

    st.image("https://upload.wikimedia.org/wikipedia/commons/e/ee/Turbofan_operation_lb.svg", caption="Turbofan Engine Simulation Context")

def render_data_management():
    st.header("🛠️ Data Pipeline Management")
    
    tab1, tab2, tab3 = st.tabs(["Ingestion", "Hive Integration", "HDFS Explorer"])
    
    with tab1:
        st.subheader("Data Ingestion (Local -> HDFS)")
        di = DataIngestion()
        
        if st.button("🚀 Run Full Ingestion Pipeline"):
            with st.status("Running Ingestion Pipeline...", expanded=True) as status:
                st.write("Initializing directories...")
                status.update(label="Processing files...", state="running")
                report = di.process_and_upload()
                st.text(report)
                status.update(label="Ingestion Complete!", state="complete", expanded=False)
                
    with tab2:
        st.subheader("Hive Data Warehousing")
        hm = HiveManager()
        
        if st.button("🏗️ Initialize Hive Tables"):
            with st.spinner("Creating External Tables in Hive..."):
                results = hm.create_cmapss_tables()
                for success, msg in results:
                    icon = "✅" if success else "❌"
                    st.write(f"{icon} {msg}")
                    
        st.markdown("### Pre-defined Queries")
        query = st.selectbox("Run Analysis:", [
            "SELECT * FROM cmapss_train LIMIT 10",
            "SELECT dataset_id, count(*) FROM cmapss_train GROUP BY dataset_id",
            "SELECT unit_number, max(time_cycles) as max_life FROM cmapss_train WHERE dataset_id='FD001' GROUP BY unit_number LIMIT 5"
        ])
        if st.button("Run HiveQL"):
            success, res = hm.run_query(query)
            if success:
                st.code(res)
            else:
                st.error(res)

    with tab3:
        st.subheader("HDFS File Explorer")
        hm_hdfs = HDFSManager()
        path = st.text_input("Path", value="/bda_project/processed")
        try:
            files = hm_hdfs.list_files(path)
            if files:
                st.dataframe(pd.DataFrame(files))
            else:
                st.info("Directory empty or path invalid.")
        except Exception as e:
            st.error(f"Error: {e}")

def render_dashboard():
    st.header("📊 Fleet Dashboard")
    ms = ModelService()
    
    dataset_id = st.selectbox("Select Dataset", ["FD001", "FD002", "FD003", "FD004"])
    
    # Cache downloading data for dashboard performance
    @st.cache_data
    def load_data(ds_id):
        # We assume data is ingested. If not, this fails.
        # For dashboard, let's look at TRAIN data
        path = f"/bda_project/processed/train/{ds_id}.csv"
        try:
            return ms._get_data_from_hdfs(path)
        except:
            return None

    if st.button("Load Data"):
        with st.spinner("Fetching data from HDFS..."):
            df = load_data(dataset_id)
            if df is not None and not df.empty:
                st.session_state['dashboard_df'] = df
                st.success("Data Loaded!")
            else:
                st.error("Could not load data. Ensure pipeline is run.")

    if 'dashboard_df' in st.session_state:
        df = st.session_state['dashboard_df']
        
        # Valid units
        units = df['unit_number'].unique()
        selected_unit = st.selectbox("Select Engine Unit", units)
        
        subset = df[df['unit_number'] == selected_unit]
        
        st.subheader(f"Engine Unit {selected_unit} Analysis")
        
        # Plot Sensors
        sensor_cols = [f"sensor_{i}" for i in range(1, 22)]
        selected_sensors = st.multiselect("Select Sensors to Visualize", sensor_cols, default=["sensor_2", "sensor_3", "sensor_4"])
        
        if selected_sensors:
            fig = px.line(subset, x="time_cycles", y=selected_sensors, title=f"Sensor Readings over Time (Unit {selected_unit})")
            st.plotly_chart(fig, use_container_width=True)
            
        # Op Settings
        st.subheader("Operational Settings")
        fig2 = px.scatter(subset, x="op_setting_1", y="op_setting_2", color="time_cycles", title="Operational Setting Drift")
        st.plotly_chart(fig2, use_container_width=True)

def render_predictive_maintenance():
    st.header("🤖 Predictive Maintenance (RUL)")
    
    ms = ModelService()
    dataset_id = st.selectbox("Target Dataset", ["FD001", "FD002", "FD003", "FD004"], key="pred_ds")
    
    col_train, col_infer = st.columns(2)
    
    with col_train:
        st.subheader("Model Training")
        st.info("Train a Random Forest Regressor on the Training set.")
        if st.button("Train Model"):
            with st.spinner(f"Training model for {dataset_id}..."):
                success, msg = ms.train_model(dataset_id)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)
                    
    with col_infer:
        st.subheader("RUL Inference")
        st.info("Predict RUL for engines in the Test set.")
        unit_to_test = st.number_input("Unit Number (Test Set)", min_value=1, value=1)
        
        if st.button("Predict RUL"):
            with st.spinner("Running Inference..."):
                res, msg = ms.predict_rul(dataset_id, unit_to_test)
                if res is not None:
                    # Get last prediction (current state RUL)
                    current_rul = res.iloc[-1]['predicted_rul']
                    current_cycle = res.iloc[-1]['time_cycles']
                    
                    st.metric(label="Predicted Remaining Useful Life (Cycles)", value=f"{current_rul:.2f}")
                    st.metric(label="Current Age", value=f"{current_cycle}")
                    
                    st.write("RUL Projection:")
                    st.line_chart(res.set_index("time_cycles")['predicted_rul'])
                else:
                    st.error(msg)

# Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Home", "Data Pipeline", "Fleet Dashboard", "Predictive Maintenance"])

check_services()

if page == "Home":
    render_home()
elif page == "Data Pipeline":
    render_data_management()
elif page == "Fleet Dashboard":
    render_dashboard()
elif page == "Predictive Maintenance":
    render_predictive_maintenance()
