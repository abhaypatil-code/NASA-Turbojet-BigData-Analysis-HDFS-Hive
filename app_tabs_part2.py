# Continuation of app.py - Remaining Tabs
# This contains Tab 2-8 implementations

# ==================== TAB 2: DATA INGESTION ====================
with tab2:
    st.header("📤 Data Upload & Ingestion Pipeline")
    
    st.markdown("""
    Upload and process CMAPSS datasets into MongoDB and HDFS for analysis.
    The pipeline automatically cleans data, adds metadata, and distributes to storage systems.
    """)
    
    col_upload, col_status = st.columns([2, 1])
    
    with col_upload:
        st.subheader("🚀 Ingestion Controls")
        
        # Dataset selection
        selected_datasets = st.multiselect(
            "Select datasets to ingest",
            options=get_all_datasets(),
            default=get_all_datasets(),
            help="Choose which datasets to process"
        )
        
        st.markdown("**Ingestion targets:**")
        st.markdown("- 🍃 **MongoDB**: `BDA_Project.sensors`collection")
        st.markdown("- 🐘 **HDFS**: `/bda_project/processed/` directory structure")
        
        if st.button("▶️ Run Full Ingestion Pipeline", type="primary", use_container_width=True):
            with st.status("Running ingestion pipeline...", expanded=True) as status:
                st.write("🔧 Initializing directories...")
                
                try:
                    # Run ingestion
                    st.write("📂 Processing files...")
                    report = svc.ingestion.process_and_upload()
                    
                    st.write("✅ Processing complete!")
                    status.update(label="Ingestion Complete!", state="complete", expanded=False)
                    
                    # Show report
                    with st.expander("📋 View Detailed Report", expanded=True):
                        st.code(report)
                    
                    show_status_box("Data ingestion successful! Data is now available in MongoDB and HDFS.", "success")
                    
                except Exception as e:
                    status.update(label="Ingestion Failed", state="error")
                    show_status_box(f"Error during ingestion: {str(e)}", "error")
    
    with col_status:
        st.subheader("📊 Storage Summary")
        
        # MongoDB Summary
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
                        st.caption(ds_data.get('description', ''))
        else:
            st.warning("MongoDB not connected")
        
        # HDFS Summary
        if hdfs_ok:
            st.markdown("**HDFS Storage:**")
            storage = svc.hdfs.get_storage_summary()
            total_mb = storage.get('total', 0) / (1024  * 1024)
            st.metric("Total Size", f"{total_mb:.2f} MB")

# ==================== TAB 3: DATA EXPLORATION ====================
with tab3:
    st.header("🔍 Data Exploration & Visualization")
    
    # Dataset selector
    dataset_id = st.selectbox("Select Dataset", get_all_datasets(), key="explore_dataset")
    dataset_meta = DATASET_METADATA.get(dataset_id, {})
    
    # Display metadata
    col_meta1, col_meta2, col_meta3 = st.columns(3)
    col_meta1.metric("Operating Conditions", dataset_meta.get('operating_conditions', 'N/A'))
    col_meta2.metric("Fault Modes", dataset_meta.get('fault_modes', 'N/A'))
    col_meta3.metric("Train Engines", dataset_meta.get('train_engines', 'N/A'))
    
    st.info(f"**Description:** {dataset_meta.get('description', 'No description available')}")
    
    st.markdown("---")
    
    tab_explore_viz, tab_explore_stats, tab_explore_unit = st.tabs([
        "📈 Sensor Trends", "📊 Statistics", "🔬 Unit Deep Dive"
    ])
    
    with tab_explore_viz:
        st.subheader("Sensor Trend Visualization")
        
        if mongo_ok:
            # Get aggregated data
            avg_data = svc.mongo.get_avg_sensors_per_unit(dataset_id, "train")
            
            if avg_data:
                df_avg = pd.DataFrame(avg_data)
                
                # Select sensors to visualize
                available_sensors = [col for col in df_avg.columns if col.startswith('avg_sensor_')]
                selected_sensors = st.multiselect(
                    "Select sensors to visualize",
                    options=available_sensors,
                    default=available_sensors[:4] if len(available_sensors) >= 4 else available_sensors
                )
                
                if selected_sensors:
                    # Create scatter plot
                    fig = px.scatter(
                        df_avg,
                        x='unit_number',
                        y=selected_sensors,
                        title=f"Average Sensor Values per Engine Unit ({dataset_id})",
                        labels={'value': 'Sensor Average', 'unit_number': 'Engine Unit'},
                        height=500
                    )
                    fig.update_layout(template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Correlation heatmap
                    if len(selected_sensors) > 1:
                        st.subheader("Sensor Correlation Heatmap")
                        corr_matrix = df_avg[selected_sensors].corr()
                        
                        fig_corr = px.imshow(
                            corr_matrix,
                            title="Sensor Correlation Matrix",
                            color_continuous_scale="RdBu_r",
                            aspect="auto"
                        )
                        fig_corr.update_layout(template="plotly_dark")
                        st.plotly_chart(fig_corr, use_container_width=True)
            else:
                st.warning("No data available. Please run data ingestion first.")
        else:
            st.error("MongoDB connection required for exploration")
    
    with tab_explore_stats:
        st.subheader("Statistical Summary")
        
        if mongo_ok:
            with st.spinner("Calculating statistics..."):
                stats = svc.mongo.get_sensor_statistics(dataset_id, "train", CRITICAL_SENSORS)
                
                if stats:
                    # Convert to DataFrame for display
                    stats_data = []
                    for sensor in CRITICAL_SENSORS:
                        stats_data.append({
                            "Sensor": sensor,
                            "Min": round(stats.get(f"{sensor}_min", 0), 2),
                            "Max": round(stats.get(f"{sensor}_max", 0), 2),
                            "Mean": round(stats.get(f"{sensor}_avg", 0), 2),
                            "Std Dev": round(stats.get(f"{sensor}_std", 0), 2)
                        })
                    
                    df_stats = pd.DataFrame(stats_data)
                    st.dataframe(df_stats, use_container_width=True, hide_index=True)
                    
                    # Visualization
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='Min', x=df_stats['Sensor'], y=df_stats['Min']))
                    fig.add_trace(go.Bar(name='Max', x=df_stats['Sensor'], y=df_stats['Max']))
                    fig.add_trace(go.Bar(name='Mean', x=df_stats['Sensor'], y=df_stats['Mean']))
                    
                    fig.update_layout(
                        title="Sensor Statistics Comparison",
                        barmode='group',
                        template="plotly_dark",
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No statistical data available")
        else:
            st.error("MongoDB connection required")
    
    with tab_explore_unit:
        st.subheader("🔬 Individual Engine Unit Analysis")
        
        unit_id = st.number_input("Enter Unit ID", min_value=1, max_value=300, value=1)
        
        if st.button("Load Unit Data", use_container_width=True):
            if mongo_ok:
                with st.spinner(f"Loading data for Unit {unit_id}..."):
                    trends = svc.mongo.get_sensor_trends(unit_id, dataset_id, "train")
                    
                    if trends:
                        df_trends = pd.DataFrame(trends)
                        
                        st.success(f"Loaded {len(df_trends)} cycles for Unit {unit_id}")
                        
                        # Display key metrics
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Total Cycles", df_trends['time_cycles'].max())
                        col2.metric("Avg HPC Temp", round(df_trends['sensor_3'].mean(), 2))
                        col3.metric("Avg HPC Pressure", round(df_trends['sensor_11'].mean(), 2))
                        
                        # Sensor selection for plotting
                        sensors_to_plot = st.multiselect(
                            "Select sensors to plot",
                            options=[f"sensor_{i}" for i in range(1, 22)],
                            default=["sensor_2", "sensor_3", "sensor_11"],
                            key="unit_sensors"
                        )
                        
                        if sensors_to_plot:
                            fig = px.line(
                                df_trends,
                                x='time_cycles',
                                y=sensors_to_plot,
                                title=f"Sensor Trends for Unit {unit_id} ({dataset_id})",
                                labels={'value': 'Sensor Value', 'time_cycles': 'Operational Cycle'},
                                height=500
                            )
                            fig.update_layout(template="plotly_dark")
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning(f"No data found for Unit {unit_id}")
            else:
                st.error("MongoDB connection required")

# Continue in next message due to length...
