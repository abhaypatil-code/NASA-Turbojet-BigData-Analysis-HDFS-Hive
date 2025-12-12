"""
Enhanced Hive Manager for CMAPSS Data Warehousing
Provides HiveQL query execution and pre-built analytical queries
"""

import subprocess
import pandas as pd
from backend.config import (
    get_hive_cmd_prefix, HDFS_ROOT, HDFS_DIRS, CMAPSS_SCHEMA,
    HIVE_DATABASE, HIVE_TABLES, DATASET_METADATA
)


class HiveManager:
    """Manages Hive operations including table creation and query execution"""
    
    def __init__(self):
        self.cmd_prefix = get_hive_cmd_prefix()
        self.database = HIVE_DATABASE
        self.tables = HIVE_TABLES
    
    def run_query(self, query, parse_output=False):
        """
        Execute HiveQL query via command line
        
        Args:
            query: HiveQL query string
            parse_output: Whether to parse output into DataFrame
        
        Returns:
            tuple: (success, output/dataframe)
        """
        try:
            # Clean query
            clean_query = query.replace('\n', ' ').strip()
            
            # Add USE database statement
            full_query = f"USE {self.database}; {clean_query}"
            
            # Build command
            cmd_str = " ".join(self.cmd_prefix) + f' "{full_query}"'
            
            result = subprocess.check_output(
                cmd_str, shell=True, stderr=subprocess.STDOUT, text=True
            )
            
            # If requested, try to parse output as DataFrame
            if parse_output:
                df = self._parse_hive_output(result)
                return True, df
            
            return True, result
            
        except subprocess.CalledProcessError as e:
            error_msg = e.output if hasattr(e, 'output') and e.output else str(e)
            return False, error_msg
        except Exception as e:
            return False, str(e)
    
    def _parse_hive_output(self, output):
        """
        Parse Hive query output into pandas DataFrame
        
        Args:
            output: Raw Hive output string
        
        Returns:
            DataFrame or None
        """
        try:
            lines = output.strip().split('\n')
            
            # Filter out Hive system messages (usually start with specific patterns)
            data_lines = [l for l in lines if l and not l.startswith(('Time taken', 'OK', 'WARN', 'INFO'))]
            
            if not data_lines:
                return None
            
            # Simple TSV parsing (Hive default output format)
            rows = [line.split('\t') for line in data_lines]
            
            if len(rows) > 0:
                return pd.DataFrame(rows[1:], columns=rows[0]) if len(rows) > 1 else pd.DataFrame()
            
            return None
        except:
            return None
    
    def create_database(self):
        """Create Hive database if not exists"""
        query = f"CREATE DATABASE IF NOT EXISTS {self.database}"
        return self.run_query(query)
    
    def create_cmapss_tables(self):
        """
        Create optimized external tables for CMAPSS datasets
        Includes partitioning and proper schema
        """
        results = []
        
        # Create database first
        success, msg = self.create_database()
        results.append((success, f"Database: {msg}"))
        
        # Build column schema
        cols = CMAPSS_SCHEMA['columns']
        hive_cols = []
        
        # Define proper types
        hive_cols.append("unit_number INT")
        hive_cols.append("time_cycles INT")
        hive_cols.append("op_setting_1 DOUBLE")
        hive_cols.append("op_setting_2 DOUBLE")
        hive_cols.append("op_setting_3 DOUBLE")
        
        # All sensors as DOUBLE
        for i in range(1, 22):
            hive_cols.append(f"sensor_{i} DOUBLE")
        
        # Add metadata columns
        hive_cols.append("dataset_id STRING")
        hive_cols.append("dataset_type STRING")
        
        schema_str = ", ".join(hive_cols)
        
        # Drop existing tables
        drop_queries = [
            "DROP TABLE IF EXISTS cmapss_train",
            "DROP TABLE IF EXISTS cmapss_test",
            "DROP TABLE IF EXISTS cmapss_rul"
        ]
        
        for q in drop_queries:
            success, msg = self.run_query(q)
            results.append((success, f"Drop: {msg[:100] if msg else 'OK'}"))
        
        # Create Train Table with partitioning
        train_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmapss_train (
            {schema_str}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{HDFS_DIRS['train']}'
        TBLPROPERTIES ('skip.header.line.count'='0')
        """
        
        success, msg = self.run_query(train_query)
        results.append((success, f"Train Table: {'Created' if success else msg[:100]}"))
        
        # Create Test Table
        test_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmapss_test (
            {schema_str}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{HDFS_DIRS['test']}'
        TBLPROPERTIES ('skip.header.line.count'='0')
        """
        
        success, msg = self.run_query(test_query)
        results.append((success, f"Test Table: {'Created' if success else msg[:100]}"))
        
        # Create RUL Table
        rul_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cmapss_rul (
            rul DOUBLE,
            dataset_id STRING,
            dataset_type STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{HDFS_DIRS['rul']}'
        TBLPROPERTIES ('skip.header.line.count'='0')
        """
        
        success, msg = self.run_query(rul_query)
        results.append((success, f"RUL Table: {'Created' if success else msg[:100]}"))
        
        return results
    
    # ==================== PRE-BUILT ANALYTICAL QUERIES ====================
    
    def get_prebuilt_queries(self):
        """
        Returns dictionary of 20 pre-built HiveQL queries for comprehensive analysis
        Each query demonstrates different Hive capabilities
        """
        queries = {
            # === BASIC FILTERING & GROUPING ===
            "Q1_filter_by_dataset": {
                "name": "Filter Engines by Dataset",
                "description": "Retrieve all data for a specific dataset (e.g., FD001)",
                "query": "SELECT * FROM cmapss_train WHERE dataset_id = 'FD001' LIMIT 100",
                "category": "Filtering"
            },
            
            "Q2_group_by_unit": {
                "name": "Group by Engine Unit",
                "description": "Count cycles per engine unit",
                "query": """
                    SELECT unit_number, COUNT(*) as cycle_count, MAX(time_cycles) as max_cycle
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001'
                    GROUP BY unit_number
                    ORDER BY max_cycle DESC
                    LIMIT 20
                """,
                "category": "Grouping"
            },
            
            "Q3_avg_sensors_per_engine": {
                "name": "Average Sensors per Engine",
                "description": "Calculate average sensor values for each engine",
                "query": """
                    SELECT 
                        unit_number,
                        AVG(sensor_2) as avg_temp_lpc,
                        AVG(sensor_3) as avg_temp_hpc,
                        AVG(sensor_11) as avg_pressure_hpc,
                        AVG(sensor_12) as avg_fuel_ratio,
                        MAX(time_cycles) as total_cycles
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001'
                    GROUP BY unit_number
                    ORDER BY total_cycles DESC
                    LIMIT 50
                """,
                "category": "Aggregation"
            },
            
            # === ADVANCED AGGREGATIONS ===
            "Q4_sensor_variability": {
                "name": "Top Engines by Sensor Variability",
                "description": "Identify engines with highest sensor standard deviation",
                "query": """
                    SELECT 
                        unit_number,
                        STDDEV_POP(sensor_11) as std_pressure,
                        STDDEV_POP(sensor_8) as std_fan_speed,
                        STDDEV_POP(sensor_9) as std_core_speed,
                        MAX(time_cycles) as lifecycle
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001'
                    GROUP BY unit_number
                    HAVING STDDEV_POP(sensor_11) > 0
                    ORDER BY std_pressure DESC
                    LIMIT 10
                """,
                "category": "Statistical Analysis"
            },
            
            "Q5_degradation_rate": {
                "name": "Sensor Degradation Over Cycles",
                "description": "Calculate degradation rate for critical sensors",
                "query": """
                    SELECT 
                        CASE 
                            WHEN time_cycles < 50 THEN 'Early'
                            WHEN time_cycles < 100 THEN 'Mid'
                            ELSE 'Late'
                        END as lifecycle_stage,
                        AVG(sensor_3) as avg_hpc_temp,
                        AVG(sensor_11) as avg_hpc_pressure,
                        AVG(sensor_14) as avg_core_speed,
                        COUNT(*) as observation_count
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001'
                    GROUP BY CASE 
                            WHEN time_cycles < 50 THEN 'Early'
                            WHEN time_cycles < 100 THEN 'Mid'
                            ELSE 'Late'
                        END
                """,
                "category": "Degradation Analysis"
            },
            
            # === MULTI-CONDITION ANALYSIS ===
            "Q6_operational_conditions": {
                "name": "Aggregate by Operating Conditions",
                "description": "Group data by operational setting ranges",
                "query": """
                    SELECT 
                        ROUND(op_setting_1, 0) as altitude_group,
                        ROUND(op_setting_2, 1) as mach_group,
                        COUNT(DISTINCT unit_number) as engine_count,
                        AVG(sensor_11) as avg_pressure,
                        AVG(sensor_12) as avg_fuel_ratio
                    FROM cmapss_train
                    WHERE dataset_id = 'FD002'
                    GROUP BY ROUND(op_setting_1, 0), ROUND(op_setting_2, 1)
                    ORDER BY engine_count DESC
                    LIMIT 20
                """,
                "category": "Condition-Based"
            },
            
            "Q7_similar_profiles": {
                "name": "Find Engines with Similar Profiles",
                "description": "Identify engines operating under similar conditions",
                "query": """
                    SELECT 
                        unit_number,
                        AVG(op_setting_1) as avg_altitude,
                        AVG(op_setting_2) as avg_mach,
                        AVG(op_setting_3) as avg_tra,
                        MAX(time_cycles) as lifecycle
                    FROM cmapss_train
                    WHERE dataset_id = 'FD002'
                    GROUP BY unit_number
                    HAVING AVG(op_setting_1) BETWEEN 0 AND 10
                    ORDER BY avg_altitude, avg_mach
                    LIMIT 30
                """,
                "category": "Pattern Recognition"
            },
            
            # === SENSOR CORRELATIONS ===
            "Q8_sensor_correlation": {
                "name": "Sensor Correlation Analysis",
                "description": "Analyze relationships between sensors",
                "query": """
                    SELECT 
                        dataset_id,
                        CORR(sensor_8, sensor_9) as fan_core_speed_corr,
                        CORR(sensor_2, sensor_3) as lpc_hpc_temp_corr,
                        CORR(sensor_11, sensor_12) as pressure_fuel_corr
                    FROM cmapss_train
                    GROUP BY dataset_id
                """,
                "category": "Correlation"
            },
            
            # === ANOMALY DETECTION ===
            "Q9_anomalous_readings": {
                "name": "Identify Anomalous Sensor Readings",
                "description": "Find readings beyond 2 standard deviations",
                "query": """
                    WITH stats AS (
                        SELECT 
                            AVG(sensor_11) as mean_pressure,
                            STDDEV_POP(sensor_11) as std_pressure
                        FROM cmapss_train
                        WHERE dataset_id = 'FD001'
                    )
                    SELECT 
                        t.unit_number,
                        t.time_cycles,
                        t.sensor_11,
                        s.mean_pressure,
                        ABS(t.sensor_11 - s.mean_pressure) / s.std_pressure as z_score
                    FROM cmapss_train t
                    CROSS JOIN stats s
                    WHERE dataset_id = 'FD001'
                        AND ABS(t.sensor_11 - s.mean_pressure) > 2 * s.std_pressure
                    ORDER BY z_score DESC
                    LIMIT 50
                """,
                "category": "Anomaly Detection"
            },
            
            # === RUL ANALYSIS ===
            "Q10_rul_statistics": {
                "name": "RUL Statistics by Dataset",
                "description": "Calculate RUL distribution statistics",
                "query": """
                    SELECT 
                        dataset_id,
                        COUNT(*) as engine_count,
                        AVG(rul) as avg_rul,
                        MIN(rul) as min_rul,
                        MAX(rul) as max_rul,
                        PERCENTILE_APPROX(rul, 0.5) as median_rul,
                        STDDEV_POP(rul) as std_rul
                    FROM cmapss_rul
                    GROUP BY dataset_id
                    ORDER BY dataset_id
                """,
                "category": "RUL Analysis"
            },
            
            # === WINDOWED AGGREGATIONS ===
            "Q11_cycle_windows": {
                "name": "Time-Windowed Aggregations",
                "description": "Aggregate sensors by cycle ranges",
                "query": """
                    SELECT 
                        FLOOR(time_cycles / 25) * 25 as cycle_window,
                        COUNT(*) as observation_count,
                        AVG(sensor_3) as avg_hpc_temp,
                        AVG(sensor_11) as avg_hpc_pressure,
                        AVG(sensor_14) as avg_core_speed
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001' AND unit_number <= 10
                    GROUP BY FLOOR(time_cycles / 25) * 25
                    ORDER BY cycle_window
                """,
                "category": "Window Functions"
            },
            
            # === CUMULATIVE METRICS ===
            "Q12_cumulative_drift": {
                "name": "Cumulative Sensor Drift",
                "description": "Calculate cumulative change in sensors over time",
                "query": """
                    SELECT 
                        unit_number,
                        time_cycles,
                        sensor_11,
                        sensor_11 - FIRST_VALUE(sensor_11) OVER (
                            PARTITION BY unit_number 
                            ORDER BY time_cycles
                        ) as pressure_drift
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001' AND unit_number = 1
                    ORDER BY time_cycles
                    LIMIT 100
                """,
                "category": "Temporal Analysis"
            },
            
            # === FAILURE PREDICTION ===
            "Q13_approaching_failure": {
                "name": "Engines Approaching Failure Threshold",
                "description": "Identify engines with critical sensor values",
                "query": """
                    WITH thresholds AS (
                        SELECT 
                            PERCENTILE_APPROX(sensor_3, 0.95) as temp_threshold,
                            PERCENTILE_APPROX(sensor_11, 0.05) as pressure_threshold
                        FROM cmapss_train
                        WHERE dataset_id = 'FD001'
                    )
                    SELECT 
                        t.unit_number,
                        MAX(t.time_cycles) as current_cycle,
                        AVG(t.sensor_3) as avg_temp,
                        AVG(t.sensor_11) as avg_pressure,
                        th.temp_threshold,
                        th.pressure_threshold
                    FROM cmapss_train t
                    CROSS JOIN thresholds th
                    WHERE t.dataset_id = 'FD001'
                    GROUP BY t.unit_number, th.temp_threshold, th.pressure_threshold
                    HAVING AVG(t.sensor_3) > th.temp_threshold 
                        OR AVG(t.sensor_11) < th.pressure_threshold
                    ORDER BY avg_temp DESC
                    LIMIT 20
                """,
                "category": "Predictive"
            },
            
            # === CROSS-DATASET COMPARISON ===
            "Q14_dataset_comparison": {
                "name": "Compare Sensor Behavior Across Datasets",
                "description": "Compare sensor statistics across all datasets",
                "query": """
                    SELECT 
                        dataset_id,
                        COUNT(DISTINCT unit_number) as engines,
                        AVG(sensor_2) as avg_lpc_temp,
                        AVG(sensor_3) as avg_hpc_temp,
                        AVG(sensor_11) as avg_pressure,
                        AVG(sensor_14) as avg_core_speed,
                        STDDEV_POP(sensor_11) as pressure_variability
                    FROM cmapss_train
                    GROUP BY dataset_id
                    ORDER BY dataset_id
                """,
                "category": "Comparative"
            },
            
            # === PERFORMANCE METRICS ===
            "Q15_efficiency_metrics": {
                "name": "Engine Efficiency Metrics",
                "description": "Calculate efficiency indicators per engine",
                "query": """
                    SELECT 
                        unit_number,
                        AVG(sensor_10) as avg_epr,
                        AVG(sensor_12) as avg_fuel_ratio,
                        AVG(sensor_15) as avg_bypass_ratio,
                        AVG(sensor_3 / sensor_2) as temp_ratio_hpc_lpc,
                        MAX(time_cycles) as lifecycle
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001'
                    GROUP BY unit_number
                    ORDER BY avg_epr DESC
                    LIMIT 30
                """,
                "category": "Performance"
            },
            
            # === RANKING ===
            "Q16_sensor_criticality": {
                "name": "Sensor Ranking by Criticality",
                "description": "Rank sensors by coefficient of variation",
                "query": """
                    SELECT 
                        'sensor_2' as sensor_name, 
                        STDDEV_POP(sensor_2) / AVG(sensor_2) as coef_variation 
                    FROM cmapss_train WHERE dataset_id = 'FD001'
                    UNION ALL
                    SELECT 
                        'sensor_3', 
                        STDDEV_POP(sensor_3) / AVG(sensor_3)
                    FROM cmapss_train WHERE dataset_id = 'FD001'
                    UNION ALL
                    SELECT 
                        'sensor_11', 
                        STDDEV_POP(sensor_11) / AVG(sensor_11)
                    FROM cmapss_train WHERE dataset_id = 'FD001'
                    ORDER BY coef_variation DESC
                """,
                "category": "Ranking"
            },
            
            # === JOINS ===
            "Q17_train_test_join": {
                "name": "Join Train and Test Data",
                "description": "Compare training vs test patterns for same engines",
                "query": """
                    SELECT 
                        train.unit_number,
                        AVG(train.sensor_11) as avg_train_pressure,
                        AVG(test.sensor_11) as avg_test_pressure,
                        MAX(train.time_cycles) as train_cycles,
                        MAX(test.time_cycles) as test_cycles
                    FROM cmapss_train train
                    JOIN cmapss_test test 
                        ON train.unit_number = test.unit_number 
                        AND train.dataset_id = test.dataset_id
                    WHERE train.dataset_id = 'FD001'
                    GROUP BY train.unit_number
                    LIMIT 50
                """,
                "category": "Joins"
            },
            
            # === FEATURE EXTRACTION ===
            "Q18_ml_features": {
                "name": "Extract ML Features",
                "description": "Generate derived features for machine learning",
                "query": """
                    SELECT 
                        unit_number,
                        time_cycles,
                        sensor_8 / sensor_9 as fan_core_ratio,
                        sensor_3 - sensor_2 as temp_diff,
                        sensor_10 as pressure_ratio,
                        (sensor_8 + sensor_9) / 2 as avg_speed,
                        SQRT(POW(sensor_11, 2) + POW(sensor_12, 2)) as combined_metric
                    FROM cmapss_train
                    WHERE dataset_id = 'FD001' AND unit_number <= 5
                    ORDER BY unit_number, time_cycles
                    LIMIT 200
                """,
                "category": "Feature Engineering"
            },
            
            # === MATERIALIZED VIEW SIMULATION ===
            "Q19_dashboard_summary": {
                "name": "Dashboard Summary View",
                "description": "Comprehensive summary for dashboards",
                "query": """
                    SELECT 
                        dataset_id,
                        COUNT(DISTINCT unit_number) as total_engines,
                        AVG(time_cycles) as avg_cycles_per_observation,
                        SUM(CASE WHEN dataset_type = 'train' THEN 1 ELSE 0 END) as train_records,
                        SUM(CASE WHEN dataset_type = 'test' THEN 1 ELSE 0 END) as test_records,
                        AVG(sensor_3) as avg_hpc_temp,
                        AVG(sensor_11) as avg_hpc_pressure,
                        PERCENTILE_APPROX(sensor_14, 0.5) as median_core_speed
                    FROM cmapss_train
                    GROUP BY dataset_id
                    ORDER BY dataset_id
                """,
                "category": "Dashboard"
            },
            
            # === PERFORMANCE REPORT ===
            "Q20_comprehensive_report": {
                "name": "Comprehensive Performance Report",
                "description": "Generate detailed performance report per dataset",
                "query": """
                    SELECT 
                        dataset_id,
                        dataset_type,
                        COUNT(DISTINCT unit_number) as engines,
                        COUNT(*) as total_observations,
                        MIN(time_cycles) as min_cycle,
                        MAX(time_cycles) as max_cycle,
                        AVG(time_cycles) as avg_cycle,
                        AVG(sensor_2) as avg_lpc_temp,
                        AVG(sensor_3) as avg_hpc_temp,
                        AVG(sensor_7) as avg_hpc_total_pressure,
                        AVG(sensor_8) as avg_fan_speed,
                        AVG(sensor_9) as avg_core_speed,
                        AVG(sensor_11) as avg_static_pressure,
                        AVG(sensor_12) as avg_fuel_flow_ratio,
                        STDDEV_POP(sensor_11) as pressure_std,
                        STDDEV_POP(sensor_14) as core_speed_std
                    FROM cmapss_train
                    GROUP BY dataset_id, dataset_type
                    ORDER BY dataset_id, dataset_type
                """,
                "category": "Reporting"
            }
        }
        
        return queries
    
    def execute_prebuilt_query(self, query_id):
        """
        Execute a specific pre-built query
        
        Args:
            query_id: Query identifier (e.g., 'Q1_filter_by_dataset')
        
        Returns:
            tuple: (success, result, metadata)
        """
        queries = self.get_prebuilt_queries()
        
        if query_id not in queries:
            return False, "Query ID not found", {}
        
        query_info = queries[query_id]
        query = query_info['query']
        
        success, result = self.run_query(query)
        
        metadata = {
            "name": query_info['name'],
            "description": query_info['description'],
            "category": query_info['category']
        }
        
        return success, result, metadata


if __name__ == "__main__":
    # Test Hive Manager
    hm = HiveManager()
    print("Creating Hive Tables...")
    results = hm.create_cmapss_tables()
    
    for success, msg in results:
        status = "✓" if success else "✗"
        print(f"{status} {msg}")
    
    print("\n=== Available Pre-built Queries ===")
    queries = hm.get_prebuilt_queries()
    for qid, qinfo in queries.items():
        print(f"{qid}: {qinfo['name']} ({qinfo['category']})")
