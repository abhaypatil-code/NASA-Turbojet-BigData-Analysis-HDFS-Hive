import streamlit as st
import pandas as pd
import os
import time
import shutil
from backend.mongo_manager import MongoManager
from backend.hdfs_manager import HDFSManager
from backend.mapreduce_manager import MapReduceManager
from backend.hive_manager import HiveManager
from backend.config import CMAPS_DIR, HDFS_ROOT

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
    if shutil.which("hadoop"):
        st.sidebar.success("Hadoop: Detected")
    else:
        st.sidebar.warning("Hadoop: Not Found (Simulated Mode)")

    # Check Hive
    if shutil.which("hive"):
        st.sidebar.success("Hive: Detected")
    else:
        st.sidebar.warning("Hive: Not Found")


def render_home():
    st.title("🧩 Big Data Analytics - CMAPS Project")
    st.markdown("### Integration of MongoDB, Hadoop, MapReduce, and Hive")
    st.markdown("""
    This project demonstrates a complete Big Data pipeline for Predictive Maintenance using the **NASA C-MAPSS** dataset.
    
    **Features:**
    - **MongoDB**: Ingestion of raw text data, storage, and analytics (Aggregation Pipeline).
    - **HDFS**: Distributed File System management for raw data storage.
    - **MapReduce**: Processing tasks (Statistics, Counts) running on Hadoop Streaming.
    - **Hive**: SQL interface for structured querying of HDFS data.
    """)
    
    col1, col2 = st.columns(2)
    with col1:
        st.info("### 📂 HDFS & MapReduce\nManage files and run distributed processing jobs.")
    with col2:
        st.success("### 🍃 MongoDB & Hive\nStore flexible data and run SQL analytics.")

def render_mongodb():
    st.header("🍃 MongoDB Analytics")
    mm = MongoManager()
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Data Ingestion")
        # List files in CMaps
        files = [f for f in os.listdir(CMAPS_DIR) if f.endswith(".txt")]
        selected_file = st.selectbox("Select Dataset to Ingest", files, index=files.index('train_FD004.txt') if 'train_FD004.txt' in files else 0)
        
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

        if mm.get_summary().get("total_records", 0) > 0:
            tab_corr, tab_agg = st.tabs(["Correlation Matrix", "Aggregations"])
            
            with tab_corr:
                st.write("**Sensor Correlation Matrix (First 1000 samples)**")
                data = pd.DataFrame(mm.get_correlation_data())
                if not data.empty:
                    # Keep numeric only for correlation
                    numeric_df = data.select_dtypes(include=['float64', 'int64'])
                    corr = numeric_df.corr()
                    st.dataframe(corr.style.background_gradient(cmap='coolwarm'), height=400)
            
            with tab_agg:
                st.write("**Average Sensor Readings per Unit (MongoDB Aggregation)**")
                agg_data = mm.get_avg_sensors_per_unit()
                if agg_data:
                    agg_df = pd.DataFrame(agg_data)
                    # Rename _id to unit_number for display
                    agg_df.rename(columns={"_id": "unit_number"}, inplace=True)
                    
                    st.dataframe(agg_df)
                    st.bar_chart(agg_df.set_index("unit_number")["max_cycle"])
                    st.caption("Max Cycles per Unit (Life Expectancy)")
                else:
                    st.info("No data available for aggregation.")

            # Viz 2: Unit Trends
            st.divider()
            st.write("**Unit Sensor Trends**")
            unit_id = st.number_input("Enter Unit ID", min_value=1, max_value=200, value=1)
            unit_data = mm.get_sensor_trends(unit_id)
            if unit_data:
                udf = pd.DataFrame(unit_data)
                # Plot Sensor 11 and 12
                st.line_chart(udf.set_index('time_cycles')[['sensor_11', 'sensor_12', 'sensor_13']])
            else:
                st.warning("No data for this unit.")
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
            files_local = [f for f in os.listdir(CMAPS_DIR) if f.endswith(".txt")]
            upload_file = st.selectbox("Select File to Upload", files_local)
            
            if st.button("Upload to HDFS"):
                 with st.spinner("Uploading..."):
                     local_p = os.path.join(CMAPS_DIR, upload_file)
                     success, out = hm.upload_file(local_p, f"{HDFS_ROOT}/input/{upload_file}")
                     if success: st.success("Uploaded successfully!")
                     else: st.error(f"Error: {out}")
            
            st.divider()
            
            # Delete
            del_path = st.text_input("Path to Delete", value=f"{HDFS_ROOT}/input/{upload_file}")
            if st.button("Delete File/Dir"):
                res, msg = hm.delete_file(del_path)
                if res: st.success("Deleted successfully")
                else: st.error(msg)

            st.divider()
            
            # Download
            dl_path = st.text_input("HDFS Path to Download", value=f"{HDFS_ROOT}/input/{upload_file}")
            local_dl_path = os.path.join(CMAPS_DIR, f"downloaded_{upload_file}")
            if st.button("Download from HDFS"):
                with st.spinner("Downloading..."):
                    res, msg = hm.download_file(dl_path, local_dl_path)
                    if res: st.success(f"Downloaded to {local_dl_path}")
                    else: st.error(f"Error: {msg}")
            
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
        
        selected_job = st.radio("Select Job", ["Sensor Statistics (mr_sensor_stats.py)", "Operation Count (mr_op_count.py)"])
        
        runner = st.selectbox("Runner", ["inline", "hadoop"])
        
        if runner == "inline":
            input_file = st.selectbox("Input File (Local)", [os.path.join(CMAPS_DIR, f) for f in files_local])
        else:
            input_file = st.text_input("Input File (HDFS Path)", value=f"{HDFS_ROOT}/input/{files_local[0] if files_local else 'train_FD004.txt'}")
        
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
    
    # Predefined Queries
    predefined_queries = [
        "SELECT * FROM cmaps_sensors LIMIT 10",
        "SELECT COUNT(*) FROM cmaps_sensors",
        "SELECT unit_number, COUNT(*) as cycles FROM cmaps_sensors GROUP BY unit_number LIMIT 20",
        "SELECT unit_number, AVG(s11) as avg_pressure FROM cmaps_sensors GROUP BY unit_number LIMIT 10",
        "SELECT unit_number, MAX(time_cycles) as max_life FROM cmaps_sensors GROUP BY unit_number ORDER BY max_life DESC LIMIT 5",
        "SELECT AVG(s11), AVG(s12), AVG(s13) FROM cmaps_sensors",
        "SELECT unit_number, s2 FROM cmaps_sensors WHERE s2 > 643.0 LIMIT 20",
        "SELECT DISTINCT unit_number FROM cmaps_sensors LIMIT 20",
        "SELECT unit_number, STDDEV(s11) as std_p FROM cmaps_sensors GROUP BY unit_number LIMIT 10",
        "SELECT time_cycles, AVG(s14) FROM cmaps_sensors GROUP BY time_cycles ORDER BY time_cycles LIMIT 20",
        "SELECT unit_number, MIN(s11), MAX(s11) FROM cmaps_sensors GROUP BY unit_number LIMIT 10",
        "SELECT * FROM cmaps_sensors WHERE unit_number = 1 AND time_cycles < 20",
        "SELECT unit_number, s4+s5 as combined_temp FROM cmaps_sensors LIMIT 10",
        "SELECT COUNT(DISTINCT unit_number) FROM cmaps_sensors",
        "SELECT unit_number, AVG(s9) FROM cmaps_sensors GROUP BY unit_number HAVING AVG(s9) > 8000 LIMIT 10"
    ]
    
    selected_query = st.selectbox("Choose a Predefined Query", predefined_queries)
    query_input = st.text_area("Or Edit Query", value=selected_query)
    
    if st.button("Run Hive Query"):
        with st.spinner("Executing..."):
            res, out = hvm.run_query(query_input)
            if res:
                st.success("Query Executed")
                st.code(out)
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
