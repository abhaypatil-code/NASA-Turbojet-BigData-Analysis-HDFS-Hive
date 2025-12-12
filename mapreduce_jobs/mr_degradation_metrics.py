#!/usr/bin/env python
"""
MapReduce Job: Degradation Metrics
Computes engine degradation metrics based on sensor trends
"""


# Compatibility fix for Python 3.14+ where pipes module is removed
try:
    import pipes
except ImportError:
    import sys
    import shlex
    from types import ModuleType
    pipes = ModuleType("pipes")
    pipes.quote = shlex.quote
    sys.modules["pipes"] = pipes

from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class MRDegradationMetrics(MRJob):
    """
    MapReduce job to calculate degradation metrics for engines
    Focuses on critical sensors that indicate wear and degradation
    """
    
    # Critical sensors for degradation analysis
    CRITICAL_SENSOR_INDICES = {
        'temp_hpc': 4,      # sensor_3 (HPC outlet temp) - index 5 in CSV
        'pressure_hpc': 12,  # sensor_11 (HPC static pressure) - index 13
        'fan_speed': 9,      # sensor_8 (Physical fan speed) - index 11
        'core_speed': 10,    # sensor_9 (Physical core speed) - index 12
        'fuel_ratio': 13     # sensor_12 (Fuel flow ratio) - index 14
    }
    
    def mapper(self, _, line):
        """
        Map phase: Extract unit data with cycle and critical sensors
        
        Input: CSV line
        Output: (unit_number, cycle_data)
        """
        if not line or not line.strip():
            return
        
        try:
            parts = line.strip().split(',')
            
            if len(parts) < 15:
                return
            
            unit_number = int(float(parts[0]))
            time_cycles = int(float(parts[1]))
            
            # Extract critical sensor values
            sensor_data = {}
            for name, idx in self.CRITICAL_SENSOR_INDICES.items():
                try:
                    sensor_data[name] = float(parts[idx])
                except (ValueError, IndexError):
                    sensor_data[name] = 0.0
            
            # Emit: unit -> (cycle, sensor_data)
            yield (unit_number, {
                'cycle': time_cycles,
                **sensor_data
            })
            
        except (ValueError, IndexError):
            pass
    
    def reducer(self, unit_number, cycle_data_list):
        """
        Reducer: Calculate degradation metrics per unit
        
        Input: (unit_number, [cycle_data1, cycle_data2, ...])
        Output: (unit_number, degradation_metrics)
        """
        # Collect all cycle data and sort by cycle
        all_data = sorted(list(cycle_data_list), key=lambda x: x['cycle'])
        
        if len(all_data) < 2:
            return
        
        # Get early and late cycle data for comparison
        early_window = all_data[:min(10, len(all_data) // 4)]
        late_window = all_data[-min(10, len(all_data) // 4):]
        
        # Calculate averages
        def avg_sensors(window):
            n = len(window)
            if n == 0:
                return {}
            
            avgs = {}
            for sensor in self.CRITICAL_SENSOR_INDICES.keys():
                avg = sum(d.get(sensor, 0) for d in window) / n
                avgs[sensor] = avg
            return avgs
        
        early_avg = avg_sensors(early_window)
        late_avg = avg_sensors(late_window)
        
        # Calculate degradation (change from early to late)
        degradation = {}
        for sensor in self.CRITICAL_SENSOR_INDICES.keys():
            early_val = early_avg.get(sensor, 0)
            late_val = late_avg.get(sensor, 0)
            
            # Degradation as percentage change
            if early_val != 0:
                pct_change = ((late_val - early_val) / abs(early_val)) * 100
            else:
                pct_change = 0
            
            degradation['{}_pct_change'.format(sensor)] = round(pct_change, 2)
        
        # Calculate health index (simplified)
        # Higher temperature + lower pressure = worse health
        temp_increase = degradation.get('temp_hpc_pct_change', 0)
        pressure_change = degradation.get('pressure_hpc_pct_change', 0)
        
        # Health index: negative is bad (temperature up, pressure down)
        health_index = -temp_increase + pressure_change
        
        result = {
            'unit': unit_number,
            'total_cycles': all_data[-1]['cycle'],
            'degradation': degradation,
            'health_index': round(health_index, 2),
            'early_avg_temp': round(early_avg.get('temp_hpc', 0), 2),
            'late_avg_temp': round(late_avg.get('temp_hpc', 0), 2),
            'early_avg_pressure': round(early_avg.get('pressure_hpc', 0), 2),
            'late_avg_pressure': round(late_avg.get('pressure_hpc', 0), 2)
        }
        
        yield (unit_number, json.dumps(result))
    
    def steps(self):
        """Define MapReduce steps"""
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    MRDegradationMetrics.run()
