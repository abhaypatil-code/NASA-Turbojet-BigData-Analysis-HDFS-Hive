# HiveQL Query Reference - NASA Turbojet Analytics

## Overview
This document provides detailed information about the 20 pre-built HiveQL queries implemented in the NASA Turbojet Predictive Maintenance Platform. Each query demonstrates different Hive capabilities and analytical techniques.

## Table Schema

### **cmapss_train & cmapss_test Tables**
```sql
CREATE EXTERNAL TABLE cmapss_train (
    unit_number INT,
    time_cycles INT,
    op_setting_1 DOUBLE,
    op_setting_2 DOUBLE,
    op_setting_3 DOUBLE,
    sensor_1 DOUBLE,
    sensor_2 DOUBLE,
    ... (sensors 3-21),
    dataset_id STRING,
    dataset_type STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/bda_project/processed/train';
```

### **cmapss_rul Table**
```sql
CREATE EXTERNAL TABLE cmapss_rul (
    rul DOUBLE,
    dataset_id STRING,
    dataset_type STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/bda_project/processed/rul';
```

---

## Query Catalog

### **Category: Filtering & Basic Operations**

#### Q1: Filter Engines by Dataset
**Purpose**: Retrieve all data for a specific dataset  
**Demonstrates**: Basic filtering, LIMIT clause

```sql
SELECT * FROM cmapss_train 
WHERE dataset_id = 'FD001' 
LIMIT 100;
```

**Expected Output**: First 100 records from FD001 training data  
**Use Case**: Data exploration, quality checking

---

#### Q2: Group by Engine Unit
**Purpose**: Count operational cycles per engine  
**Demonstrates**: GROUP BY, aggregate functions, ORDER BY

```sql
SELECT 
    unit_number, 
    COUNT(*) as cycle_count, 
    MAX(time_cycles) as max_cycle
FROM cmapss_train
WHERE dataset_id = 'FD001'
GROUP BY unit_number
ORDER BY max_cycle DESC
LIMIT 20;
```

**Expected Output**: Top 20 engines by total operational life  
**Use Case**: Identify long-lasting vs short-lived engines

---

### **Category: Statistical Aggregation**

#### Q3: Average Sensors per Engine
**Purpose**: Calculate average sensor values for each engine  
**Demonstrates**: Multiple aggregations, meaningful column aliases

```sql
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
LIMIT 50;
```

**Expected Output**: Sensor averages and lifecycle for top 50 engines  
**Use Case**: Pattern identification, engine comparison

---

#### Q4: Top Engines by Sensor Variability
**Purpose**: Identify engines with highest sensor standard deviation  
**Demonstrates**: STDDEV_POP, HAVING clause, filtering on aggregates

```sql
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
LIMIT 10;
```

**Expected Output**: 10 engines with highest pressure variability  
**Use Case**: Anomaly detection, unstable operation identification

---

### **Category: Degradation Analysis**

#### Q5: Sensor Degradation by Lifecycle Stage
**Purpose**: Calculate degradation rate for critical sensors  
**Demonstrates**: CASE expressions, binning continuous values

```sql
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
    END;
```

**Expected Output**: Sensor averages grouped by lifecycle stages  
**Use Case**: Degradation trend visualization, wear analysis

---

### **Category: Condition-Based Analysis**

#### Q6: Aggregate by Operating Conditions
**Purpose**: Group data by operational setting ranges  
**Demonstrates**: ROUND function, multi-column grouping

```sql
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
LIMIT 20;
```

**Expected Output**: Engine distribution across operating conditions  
**Use Case**: Multi-condition dataset analysis (FD002, FD004)

---

#### Q7: Find Engines with Similar Operational Profiles
**Purpose**: Identify engines operating under similar conditions  
**Demonstrates**: Range filtering with HAVING, BETWEEN

```sql
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
LIMIT 30;
```

**Expected Output**: Engines in specific operating envelope  
**Use Case**: Cohort analysis, condition-specific modeling

---

### **Category: Advanced Analytics**

#### Q8: Sensor Correlation Analysis
**Purpose**: Analyze relationships between sensors  
**Demonstrates**: CORR function, cross-sensor analysis

```sql
SELECT 
    dataset_id,
    CORR(sensor_8, sensor_9) as fan_core_speed_corr,
    CORR(sensor_2, sensor_3) as lpc_hpc_temp_corr,
    CORR(sensor_11, sensor_12) as pressure_fuel_corr
FROM cmapss_train
GROUP BY dataset_id;
```

**Expected Output**: Correlation coefficients per dataset  
**Use Case**: Feature selection for ML, sensor redundancy analysis

---

#### Q9: Identify Anomalous Sensor Readings
**Purpose**: Find readings beyond 2 standard deviations  
**Demonstrates**: CTE (WITH clause), CROSS JOIN, Z-score calculation

```sql
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
LIMIT 50;
```

**Expected Output**: Top 50 anomalous pressure readings  
**Use Case**: Outlier detection, data quality assessment

---

### **Category: RUL Analysis**

#### Q10: RUL Statistics by Dataset
**Purpose**: Calculate RUL distribution statistics  
**Demonstrates**: PERCENTILE_APPROX, comprehensive statistical summary

```sql
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
ORDER BY dataset_id;
```

**Expected Output**: Complete RUL statistics for all datasets  
**Use Case**: Ground truth analysis, model evaluation baseline

---

### **Category: Window Functions & Temporal Analysis**

#### Q11: Time-Windowed Aggregations
**Purpose**: Aggregate sensors by cycle ranges  
**Demonstrates**: FLOOR function for binning, windowing

```sql
SELECT 
    FLOOR(time_cycles / 25) * 25 as cycle_window,
    COUNT(*) as observation_count,
    AVG(sensor_3) as avg_hpc_temp,
    AVG(sensor_11) as avg_hpc_pressure,
    AVG(sensor_14) as avg_core_speed
FROM cmapss_train
WHERE dataset_id = 'FD001' AND unit_number <= 10
GROUP BY FLOOR(time_cycles / 25) * 25
ORDER BY cycle_window;
```

**Expected Output**: Sensor averages in 25-cycle windows  
**Use Case**: Time-series smoothing, trend analysis

---

#### Q12: Cumulative Sensor Drift
**Purpose**: Calculate cumulative change in sensors over time  
**Demonstrates**: Window functions, PARTITION BY, FIRST_VALUE

```sql
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
LIMIT 100;
```

**Expected Output**: Pressure drift trajectory for Unit 1  
**Use Case**: Degradation rate calculation, wear monitoring

---

### **Category: Predictive & Threshold-Based**

#### Q13: Engines Approaching Failure Threshold
**Purpose**: Identify engines with critical sensor values  
**Demonstrates**: Subquery, threshold-based filtering, multi-condition alerts

```sql
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
LIMIT 20;
```

**Expected Output**: 20 engines exceeding failure thresholds  
**Use Case**: Predictive maintenance alerts, risk assessment

---

### **Category: Comparative Analysis**

#### Q14: Compare Sensor Behavior Across Datasets
**Purpose**: Compare sensor statistics across all datasets  
**Demonstrates**: Cross-dataset aggregation, comparative analytics

```sql
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
ORDER BY dataset_id;
```

**Expected Output**: Sensor statistics comparison table  
**Use Case**: Dataset characterization, condition comparison

---

### **Category: Performance Metrics**

#### Q15: Engine Efficiency Metrics
**Purpose**: Calculate efficiency indicators per engine  
**Demonstrates**: Derived metrics, ratio calculations

```sql
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
LIMIT 30;
```

**Expected Output**: Top 30 engines by engine pressure ratio  
**Use Case**: Efficiency analysis, performance ranking

---

### **Category: Feature Engineering**

#### Q16-20 (Remaining Queries)
See implementation in `backend/hive_manager.py` for:
- Q16: Sensor Ranking by Criticality (UNION ALL)
- Q17: Train-Test Join Analysis (JOIN operations)
- Q18: ML Feature Extraction (derived features)
- Q19: Dashboard Summary View (comprehensive aggregation)
- Q20: Comprehensive Performance Report (multi-metric reporting)

---

## Execution Guide

### From Streamlit UI
1. Navigate to **HiveQL Queries** tab
2. Click **Initialize Hive Tables** (first time only)
3. Browse prebuilt queries by category
4. Click **Execute** button for any query
5. View results in formatted output

### From Python
```python
from backend.hive_manager import HiveManager

hive = HiveManager()

# Execute specific query
success, result, metadata = hive.execute_prebuilt_query("Q1_filter_by_dataset")

if success:
    print(metadata['name'])
    print(result)
```

### From Hive CLI
```bash
docker exec -it hive-server hive -e "SELECT * FROM cmapss_train LIMIT 10"
```

---

## Performance Optimization Tips

1. **Use LIMIT**: Always limit results during exploration
2. **Filter Early**: Apply WHERE clauses to reduce data scanned
3. **Partitioning**: Consider partitioning by `dataset_id` for large datasets
4. **Compression**: Enable compression for HDFS storage
5. **Statistics**: Run `ANALYZE TABLE` to gather statistics

---

## Common Use Cases

| Analysis Type | Recommended Queries |
|--------------|-------------------|
| Data Exploration | Q1, Q2, Q3 |
| Anomaly Detection | Q4, Q9, Q13 |
| Degradation Analysis | Q5, Q11, Q12 |
| Condition Analysis | Q6, Q7, Q14 |
| Model Development | Q8, Q15, Q18 |
| Reporting | Q19, Q20 |

---

## Troubleshooting

**Query hangs or times out:**
- Check if dataset exists in HDFS
- Add LIMIT clause for testing
- Verify Hive server is responsive

**Empty results:**
- Confirm data ingestion completed
- Check dataset_id matches query
- Verify table locations point to correct HDFS paths

**Schema errors:**
- Reinitialize tables with correct schema
- Check data format matches delimiter
- Verify all required columns present

---

For more information, see [README.md](README.md) and [architecture.md](architecture.md)
