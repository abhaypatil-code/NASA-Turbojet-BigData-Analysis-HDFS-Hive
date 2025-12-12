import streamlit as st
import pandas as pd
import os
import time
import shutil
from backend.mongo_manager import MongoManager
from backend.hdfs_manager import HDFSManager
from backend.mapreduce_manager import MapReduceManager
from backend.hive_manager import HiveManager
from backend.config import CMAPS_DIR, HDFS_ROOT, CMAPSS_SCHEMA, USE_DOCKER

# Page Config
st.set_page_config(
    page_title="Big Data Analytics Project",
    page_icon="🧩",
    layout="wide",
)

# Custom CSS for aesthetics
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
        background-color: #0d6efd;
        color: white;
    }
    .stSidebar {
        background-color: #212529;
        color: white;
    }
    h1, h2, h3 {
        color: #0d6efd;
    }
    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

def check_services():
    st.sidebar.markdown("---")
    st.sidebar.subheader("System Status")
    
    # Check MongoDB
    mm = MongoManager()
    m_ok, m_msg = mm.test_connection()
    if m_ok:
        st.sidebar.success(f"MongoDB: Connected")
    else:
        st.sidebar.error(f"MongoDB: Failed")

    # Check Hadoop
    if USE_DOCKER:
        # Simple check if docker is available
        if shutil.which("docker"):
            st.sidebar.success("Hadoop: Docker Mode")
            # Ideally check if container is running
        else:
            st.sidebar.error("Hadoop: Docker Not Found")
    else:
        if shutil.which("hadoop"):
            st.sidebar.success("Hadoop: Detected")
        else:
            st.sidebar.warning("Hadoop: Not Found (Simulated Mode)")

    # Check Hive
    hive_ok = False
    if USE_DOCKER:
        if shutil.which("docker"):
            st.sidebar.success("Hive: Docker Mode")
            hive_ok = True
    else:
        if shutil.which("hive"):
            st.sidebar.success("Hive: Detected")
            hive_ok = True
    
    if not hive_ok and not m_ok:
          st.sidebar.markdown("---")
          st.sidebar.info("⚠️ Services Check Failed?\nEnsure MongoDB and Hadoop/Hive are running.")


def render_home():
    st.title("🧩 Big Data Analytics - CMAPS Project")
    st.markdown("### Integration of MongoDB, Hadoop, MapReduce, and Hive")
    st.markdown(f"""
    This project demonstrates a complete Big Data pipeline for Predictive Maintenance using the **NASA C-MAPSS** dataset.
    
    **Dataset Schema (Dynamic):**
    - **Total Columns**: {len(CMAPSS_SCHEMA['columns'])}
    - **Key Fields**: Unit Number, Time Cycles, Operational Settings (1-3)
    - **Sensors**: 21 Sensors (Pressure, Temperature, etc.)
    """)
    
    with st.expander("View Full Schema Definition"):
        st.json(CMAPSS_SCHEMA)
    
    col1, col2 = st.columns(2)
    with col1:
        st.info("### 📂 HDFS & MapReduce\nManage files and run distributed processing jobs for stats and counting.")
    with col2:
        st.success("### 🍃 MongoDB & Hive\nSchema-flexible storage with Aggregation Pipelines + SQL Warehousing.")

def render_mongodb():
    st.header("🍃 MongoDB Analytics")
    mm = MongoManager()
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Data Ingestion")
        # List files in CMaps
        files = [f for f in os.listdir(CMAPS_DIR) if f.endswith(".txt") or f.endswith(".zip")]
        selected_file = st.selectbox("Select Dataset to Ingest", files, index=0)
        
        if st.button("Ingest Data to MongoDB"):
            with st.spinner("Ingesting data... (This may take a moment)"):
                success, msg = mm.ingest_data(os.path.join(CMAPS_DIR, selected_file))
                if success:
                    st.success(msg)
                else:
                    st.error(f"Failed: {msg}")
        
        st.divider()
        st.subheader("Collection Stats")
        summary = mm.get_summary()
        st.write(summary)

    with col2:
        st.subheader("Analytics Dashboard")
        mongo_status = mm.test_connection()
        if not mongo_status[0]:
            st.error(f"MongoDB not connected: {mongo_status[1]}")
            return

        summary_data = mm.get_summary()
        if summary_data.get("total_records", 0) > 0:
            
            tab_health, tab_corr, tab_agg = st.tabs(["Unit Health Score", "Correlation Matrix", "Aggregations"])
            
            with tab_health:
                st.write("#### Derived Health Score (Proxy)")
                st.caption("Calculated via Aggregation Pipeline: 100 - (Deviations). Higher is better.")
                
                health_data = mm.get_unit_health_scores()
                if health_data:
                    df_health = pd.DataFrame(health_data)
                    # Normalize simple metric for display
                    # Just plotting max_life vs health_index
                    st.scatter_chart(data=df_health, x='max_life', y='health_index', color='unit_number')
                    st.dataframe(df_health.head(10))
                else:
                    st.warning("Could not calculate health scores.")

            with tab_corr:
                st.write("**Sensor Correlation Matrix (First 2000 samples)**")
                data = pd.DataFrame(mm.get_correlation_data()[:2000]) 
                if not data.empty:
                    numeric_df = data.select_dtypes(include=['float64', 'int64'])
                    numeric_df = numeric_df.loc[:, numeric_df.std() > 0] # Drop constant cols
                    
                    if not numeric_df.empty:
                        corr = numeric_df.corr()
                        st.dataframe(corr.style.background_gradient(cmap='coolwarm'), height=400)
                    else:
                        st.warning("Not enough variance in data.")
                else:
                    st.info("No data available.")
            
            with tab_agg:
                st.write("**Average Sensor Readings per Unit**")
                agg_data = mm.get_avg_sensors_per_unit()
                if agg_data:
                    agg_df = pd.DataFrame(agg_data)
                    agg_df.rename(columns={"_id": "unit_number"}, inplace=True)
                    st.dataframe(agg_df)
                    st.bar_chart(agg_df.set_index("unit_number")["max_cycle"])
                    st.caption("Max Cycles per Unit (Life Expectancy)")
                else:
                    st.info("No aggregation data available.")
        else:
            st.info("No data in MongoDB. Please ingest data first.")

def render_hdfs_mr():
    st.header("🐘 Hadoop Ecosystem (HDFS & MapReduce)")
    
    tab1, tab2 = st.tabs(["HDFS File Manager", "MapReduce Jobs"])
    
    hm = HDFSManager()
    
    with tab1:
        st.subheader("HDFS Explorer")
        
        col_act, col_view = st.columns([1, 2])
        
        with col_act:
            st.info(f"Root: {HDFS_ROOT}")
            files_local = [f for f in os.listdir(CMAPS_DIR) if f.endswith(".txt") or f.endswith(".zip")]
            upload_file = st.selectbox("Select File to Upload", files_local)
            
            if st.button("Upload to HDFS"):
                 with st.spinner("Uploading..."):
                     local_p = os.path.join(CMAPS_DIR, upload_file)
                     success, out = hm.upload_and_clean_file(local_p)
                     if success: st.success("Uploaded & Cleaned successfully!")
                     else: st.error(f"Error: {out}")
            
            st.divider()
            
            # Delete
            del_path = st.text_input("Path to Delete", value=f"{HDFS_ROOT}/input/{upload_file}")
            if st.button("Delete File/Dir"):
                res, msg = hm.delete_file(del_path)
                if res: st.success("Deleted successfully")
                else: st.error(msg)
            
            if st.button("Create Root/Input Dirs"):
                hm.mkdir(f"{HDFS_ROOT}/input")
                st.success(f"Created {HDFS_ROOT}/input")
                
        with col_view:
            st.write("Current Files in HDFS:")
            # In a real run we would recursively check, but here we check root and input
            items_root = hm.list_files(HDFS_ROOT)
            items_input = hm.list_files(f"{HDFS_ROOT}/input")
            
            all_files = items_root + items_input
            if all_files:
                st.dataframe(pd.DataFrame(all_files))
            else:
                st.warning("No files found or HDFS not reachable.")

    with tab2:
        st.subheader("MapReduce Execution")
        mrm = MapReduceManager()
        
        selected_job = st.radio("Select Job", [
            "Sensor Statistics (mr_sensor_stats.py) - Min/Max/Avg for Sensor 11", 
            "Operation Count (mr_op_count.py) - Rows per Unit"
        ])
        
        runner = st.selectbox("Runner", ["inline", "hadoop"])
        
        if runner == "inline":
            input_file = st.selectbox("Input File (Local)", [os.path.join(CMAPS_DIR, f) for f in files_local if f.endswith('.txt')])
        else:
            # Assumes uploaded
            input_file = st.text_input("Input File (HDFS Path)", value=f"{HDFS_ROOT}/input/train_FD004.txt")
        
        if st.button("Run MapReduce Job"):
            job_script = "mr_sensor_stats.py" if "Sensor" in selected_job else "mr_op_count.py"
            
            with st.spinner(f"Running {selected_job} on {runner}..."):
                success, output = mrm.run_job(job_script, input_file, runner)
                
                if success:
                    st.success("Job Completed Successfully!")
                    st.text_area("Job Output", output, height=300)
                else:
                    st.error("Job Failed")
                    st.text_area("Error Log", output, height=300)

def render_hive():
    st.header("🐝 Hive Data Warehouse")
    hvm = HiveManager()
    
    st.markdown("Execute HiveQL queries against the data stored in HDFS.")
    
    if st.button("Initialize Table (Drop & Create External Table)"):
        with st.spinner("Creating table..."):
            res, out = hvm.create_table()
            if res: st.success("Table 'cmaps_sensors' created successfully.")
            else: st.error(f"Error: {out}")
    
    st.markdown("### 🔍 Query Interface")
    
    # 20 Meaningful Hive Queries
    predefined_queries = [
        "SELECT * FROM cmaps_sensors LIMIT 10",
        "SELECT COUNT(*) as total_records FROM cmaps_sensors",
        "SELECT unit_number, MAX(time_cycles) as useful_life FROM cmaps_sensors GROUP BY unit_number ORDER BY useful_life DESC LIMIT 10",
        "SELECT unit_number, AVG(sensor_11) as avg_pressure FROM cmaps_sensors GROUP BY unit_number LIMIT 20",
        "SELECT unit_number, STDDEV(sensor_11) as stability_s11 FROM cmaps_sensors GROUP BY unit_number ORDER BY stability_s11 DESC LIMIT 10",
        "SELECT unit_number, AVG(sensor_4) as avg_temp FROM cmaps_sensors WHERE sensor_4 > 1400 GROUP BY unit_number LIMIT 10",
        "SELECT unit_number, COUNT(*) as cycles FROM cmaps_sensors WHERE sensor_11 > 47.5 GROUP BY unit_number",
        "SELECT time_cycles, AVG(sensor_12) as global_avg_s12 FROM cmaps_sensors GROUP BY time_cycles ORDER BY time_cycles ASC LIMIT 50",
        "SELECT unit_number, MIN(sensor_2), MAX(sensor_2), AVG(sensor_2) FROM cmaps_sensors GROUP BY unit_number LIMIT 10",
        "SELECT unit_number FROM cmaps_sensors WHERE sensor_2 > 644.0 GROUP BY unit_number",
        "SELECT count(DISTINCT unit_number) as distinct_units FROM cmaps_sensors",
        "SELECT unit_number, sensor_6+sensor_7 as sum_pressure_related FROM cmaps_sensors LIMIT 20",
        "SELECT unit_number, avg(sensor_9) FROM cmaps_sensors GROUP BY unit_number HAVING avg(sensor_9) > 9000 LIMIT 10",
        "SELECT * FROM cmaps_sensors WHERE unit_number = 1 AND time_cycles BETWEEN 100 AND 120",
        "SELECT floor(sensor_11) as bin, count(*) as freq FROM cmaps_sensors GROUP BY floor(sensor_11) ORDER BY bin",
        "SELECT unit_number, (MAX(sensor_4) - MIN(sensor_4)) as temp_range FROM cmaps_sensors GROUP BY unit_number ORDER BY temp_range DESC LIMIT 5",
        "SELECT unit_number, AVG(op_setting_1) FROM cmaps_sensors GROUP BY unit_number LIMIT 10",
        "SELECT time_cycles, AVG(sensor_11), AVG(sensor_12) FROM cmaps_sensors GROUP BY time_cycles ORDER BY time_cycles LIMIT 10",
        "SELECT unit_number FROM cmaps_sensors WHERE time_cycles > 300 GROUP BY unit_number",
        "SELECT AVG(sensor_1), STDDEV(sensor_1), AVG(sensor_2), STDDEV(sensor_2) from cmaps_sensors"
    ]
    
    selected_query = st.selectbox("Choose a Predefined Analysis", predefined_queries)
    query_input = st.text_area("Or Edit Query", value=selected_query)
    
    if st.button("Run Hive Query"):
        with st.spinner("Executing..."):
            res, out = hvm.run_query(query_input)
            if res:
                st.success("Query Executed")
                st.code(out)
                
                # Try parsing CSV output to dataframe for better view
                try:
                    from io import StringIO
                    # Hive CLI output usually tab separated or formatted
                    # This is a best effort visualization
                    df = pd.read_csv(StringIO(out), sep="\t", engine='python')
                    st.dataframe(df)
                except:
                    pass
            else:
                st.error(f"Query Failed: {out}")

# Main Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Home", "MongoDB", "Hadoop & MapReduce", "HiveSQL"])

check_services()

if page == "Home":
    render_home()
elif page == "MongoDB":
    render_mongodb()
elif page == "Hadoop & MapReduce":
    render_hdfs_mr()
elif page == "HiveSQL":
    render_hive()
