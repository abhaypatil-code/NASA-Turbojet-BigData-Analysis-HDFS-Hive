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
        background-color: #0d6efd;
        color: white;
        border-radius: 8px;
    }
    .metric-card {
        padding: 1.5rem;
        background-color: #f8f9fa;
        border-radius: 10px;
        border: 1px solid #e9ecef;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-card h3 {
        margin-bottom: 0.5rem;
        color: #495057;
    }
    .status-box {
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 10px;
    }
    .success-box { background-color: #d1e7dd; color: #0f5132; }
    .error-box { background-color: #f8d7da; color: #842029; }
</style>
""", unsafe_allow_html=True)

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

svc = st.session_state.services

def check_services():
    with st.sidebar:
        st.header("🔌 System Status")
        
        # MongoDB Check
        m_ok, max_msg = svc.mongo.test_connection()
        m_icon = "szn-leaf" # Just unicode
        st.caption(f"🍃 **MongoDB**: {'✅ Connected' if m_ok else '❌ Disconnected'}")
        
        # HDFS Check
        # Simple check by listing root
        h_ok = False
        try:
            if svc.hdfs.list_files("/"): h_ok = True
        except: pass
        
        st.caption(f"🐘 **HDFS**: {'✅ Active' if h_ok else '⚠️ Error'}")

def render_home():
    st.title("✈️ NASA Turbojet Predictive Maintenance")
    st.markdown("#### Big Data Analytics Platform")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown('<div class="metric-card"><h3>Pipeline</h3><p>Ingest & Warehouse</p></div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="metric-card"><h3>MapReduce</h3><p>Distributed Processing</p></div>', unsafe_allow_html=True)
    with col3:
        st.markdown('<div class="metric-card"><h3>Analysis</h3><p>Sensor Trends</p></div>', unsafe_allow_html=True)
    with col4:
        st.markdown('<div class="metric-card"><h3>Prediction</h3><p>RUL Inference</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.info("💡 **Getting Started**: Go to 'Data Pipeline' to ingest data first if the dashboard is empty.")
    
    st.image("https://upload.wikimedia.org/wikipedia/commons/e/ee/Turbofan_operation_lb.svg", caption="Turbofan Engine Context")

def render_data_management():
    st.header("🛠️ Data Pipeline Management")
    
    st.markdown("Manage your Big Data Lifecycle here.")
    
    tab1, tab2, tab3 = st.tabs(["🚀 Ingestion", "🐝 Hive Warehouse", "📂 HDFS Explorer"])
    
    with tab1:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.subheader("Ingest Data")
            st.write("Reads raw CMAPSS files, cleans them, adds identifiers, and uploads to:")
            st.markdown("- **HDFS**: `/bda_project/processed`")
            st.markdown("- **MongoDB**: `BDA_Project.sensors`")
            
            if st.button("Run Full Ingestion Pipeline"):
                with st.status("Running Ingestion Pipeline...", expanded=True) as status:
                    st.write("Initializing directories...")
                    status.update(label="Processing files...", state="running")
                    report = svc.ingestion.process_and_upload()
                    st.text(report)
                    status.update(label="Ingestion Complete!", state="complete", expanded=False)
        with col2:
            st.subheader("Summary Statistics")
            summary = svc.mongo.get_summary()
            st.json(summary)
            
    with tab2:
        st.subheader("Hive Data Warehousing")
        
        col_act, col_q = st.columns([1, 2])
        with col_act:
            if st.button("Initialize Tables"):
                with st.spinner("Creating External Tables..."):
                    results = svc.hive.create_cmapss_tables()
                    for success, msg in results:
                        icon = "✅" if success else "❌"
                        st.write(f"{icon} {msg}")
        
        with col_q:
            st.markdown("**Run Ad-Hoc HiveQL**")
            query = st.text_area("SQL Query", "SELECT * FROM cmapss_train LIMIT 5")
            if st.button("Execute HiveQL"):
                success, res = svc.hive.run_query(query)
                if success:
                    st.code(res)
                else:
                    st.error(res)

    with tab3:
        st.subheader("File Explorer")
        path = st.text_input("HDFS Path", value="/bda_project/processed")
        if st.button("List Files"):
            try:
                files = svc.hdfs.list_files(path)
                if files:
                    st.dataframe(pd.DataFrame(files))
                else:
                    st.warning("Directory empty.")
            except Exception as e:
                st.error(f"Error: {e}")

def render_mapreduce():
    st.header("⚡ MapReduce Jobs")
    st.markdown("Run distributed processing jobs on the dataset.")

    job_options = {
        "mr_op_count.py": "Count Records per Unit",
        "mr_sensor_stats.py": "Calculate Sensor Statistics (Min/Max/Avg)",
        "mr_rul_avg.py": "Average RUL (Success Proxy) by Op Setting"
    }

    selected_job = st.selectbox("Select Job", list(job_options.keys()), format_func=lambda x: f"{x} - {job_options[x]}")
    
    # Input file selection (Simulated for assignment)
    # Ideally pick from HDFS, but for 'inline' runner we need local file or allow mrjob to handle it.
    # We will point to a raw file in CMAPS_DIR for demonstration if running inline.
    
    input_file = st.selectbox("Input File", [os.path.join(CMAPS_DIR, "train_FD001.txt"), os.path.join(HDFS_ROOT, "processed/train/FD001.csv")])
    runner = st.radio("Execution Mode", ["inline", "local", "hadoop"], help="'inline' runs in this process. 'hadoop' submits to cluster.")

    if st.button("Run Job"):
        if runner == "hadoop" and not input_file.startswith("/"):
             st.warning("For Hadoop mode, input file usually should be HDFS path (starts with /).")
        
        with st.spinner(f"Running {selected_job} ({runner})..."):
            success, output = svc.mr.run_job(selected_job, input_file, runner=runner)
            
            if success:
                st.success("Job Completed!")
                with st.expander("Show Output", expanded=True):
                    st.text(output)
            else:
                st.error("Job Failed")
                st.code(output)

def render_dashboard():
    st.header("📊 Fleet Dashboard")
    
    dataset_id = st.selectbox("Select Dataset", ["FD001", "FD002", "FD003", "FD004"])
    
    # Try fetching from MongoDB for speed
    # We need a method in ModelService or MongoManager to get full DF for dashboard
    # Or we can just use MongoManager directly.
    
    with st.spinner("Loading Data..."):
        # Check if we have data in Mongo for this dataset
        # Limiting to 5000 records for performance if grabbing raw
        # Ideally we aggregate in Mongo.
        
        # Let's show Aggregated Stats from Mongo if available
        conn, _ = svc.mongo.test_connection()
        if conn:
            st.success("Connected to MongoDB - Loading Live Analytics")
            
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Network Health")
                health_data = svc.mongo.get_unit_health_scores(dataset_id)
                if health_data:
                     df_health = pd.DataFrame(health_data)
                     fig = px.bar(df_health.head(20), x='unit_number', y='health_index', color='max_life', title="Top 20 Units by Health Index")
                     st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No data found in MongoDB for Health Score.")

            with col2:
                st.subheader("Sensor Averages")
                avg_data = svc.mongo.get_avg_sensors_per_unit(dataset_id)
                if avg_data:
                    df_avg = pd.DataFrame(avg_data)
                    fig = px.scatter(df_avg, x='avg_s11', y='max_life', size='avg_s12', title="Pressure vs Life Span")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No data found for Sensor Averages.")

            # Drill Down
            st.divider()
            st.subheader("🔍 Unit Drill Down")
            unit_id = st.number_input("Enter Unit ID", min_value=1, value=1)
            trends = svc.mongo.get_sensor_trends(unit_id, dataset_id)
            if trends:
                df_trends = pd.DataFrame(trends)
                sensors = st.multiselect("Select Sensors", [f"sensor_{i}" for i in range(1, 22)], default=["sensor_2", "sensor_3", "sensor_4"])
                if sensors:
                    fig_tr = px.line(df_trends, x='time_cycles', y=sensors, title=f"Unit {unit_id} Sensor Trends")
                    st.plotly_chart(fig_tr, use_container_width=True)
            else:
                st.warning("No trend data found for this unit.")
                
        else:
            st.warning("MongoDB not connected. Falling back to HDFS (Slower).")
            # Fallback to loading CSV from HDFS (Old Logic)
            pass 

def render_predictive_maintenance():
    st.header("🤖 Predictive Maintenance (AI)")
    
    dataset_id = st.selectbox("Target Dataset", ["FD001", "FD002", "FD003", "FD004"], key="pred_ds")
    
    mode = st.radio("Mode", ["Training", "Interference/Prediction"], horizontal=True)
    
    if mode == "Training":
        st.markdown("### Model Training")
        st.info("Trains a **Random Forest Regressor** on the historical data.")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Hyperparameters")
            n_est = st.slider("Trees", 10, 200, 100)
            
        with col2:
            st.markdown("#### Actions")
            if st.button("Start Training"):
                with st.spinner(f"Training model for {dataset_id}..."):
                    success, msg = svc.model.train_model(dataset_id)
                    if success:
                        st.balloons()
                        st.success(msg)
                    else:
                        st.error(msg)
                        
    else:
        st.markdown("### Real-time RUL Prediction")
        
        unit_to_test = st.number_input("Unit Number (Test Set)", min_value=1, value=1)
        
        if st.button("Predict Remaining Useful Life"):
            with st.spinner("Running Inference..."):
                res, msg = svc.model.predict_rul(dataset_id, unit_to_test)
                if res is not None and not res.empty:
                    # Get last prediction
                    last_row = res.iloc[-1]
                    pred_rul = last_row['predicted_rul']
                    curr_cycle = last_row['time_cycles']
                    
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Current Cycle", int(curr_cycle))
                    c2.metric("Predicted RUL", f"{pred_rul:.1f} cycles")
                    c3.metric("Total Predicted Life", f"{curr_cycle + pred_rul:.1f}")
                    
                    st.line_chart(res.set_index("time_cycles")['predicted_rul'])
                    st.caption("RUL trajectory prediction over time.")
                else:
                    st.error(msg)


# Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Home", "Data Pipeline", "MapReduce Jobs", "Fleet Dashboard", "Predictive Maintenance"])

check_services()

if page == "Home":
    render_home()
elif page == "Data Pipeline":
    render_data_management()
elif page == "MapReduce Jobs":
    render_mapreduce()
elif page == "Fleet Dashboard":
    render_dashboard()
elif page == "Predictive Maintenance":
    render_predictive_maintenance()
