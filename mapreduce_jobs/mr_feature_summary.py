#!/usr/bin/env python
"""
MapReduce Job: Feature Summary
Calculates comprehensive statistics for all 26 features (3 settings + 23 sensors)
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


class MRFeatureSummary(MRJob):
    """
    MapReduce job to calculate min, max, mean, variance for all features
    Provides comprehensive statistical summary of the dataset
    """
    
    def mapper(self, _, line):
        """
        Map phase: Extract all feature values
        
        Input: CSV line with engine data
        Output: (feature_name, value)
        """
        if not line or not line.strip():
            return
        
        try:
            parts = line.strip().split(',')
            
            # Expecting: unit, cycle, op1, op2, op3, sensor1-21, dataset_id, dataset_type
            if len(parts) < 26:
                return
            
            # Skip unit_number (index 0) and time_cycles (index 1)
            # Process operational settings (indices 2-4)
            feature_names = ['op_setting_1', 'op_setting_2', 'op_setting_3'] + \
                          ['sensor_{}'.format(i) for i in range(1, 22)]
            
            for idx, feature_name in enumerate(feature_names, start=2):
                if idx < len(parts):
                    try:
                        value = float(parts[idx])
                        yield (feature_name, value)
                    except ValueError:
                        pass
                        
        except (ValueError, IndexError):
            pass
    
    def combiner(self, feature, values):
        """
        Combiner: Pre-aggregate statistics locally
        
        Input: (feature_name, [value1, value2, ...])
        Output: (feature_name, {count, sum, min, max, sum_sq})
        """
        values_list = list(values)
        
        if not values_list:
            return
        
        count = len(values_list)
        total = sum(values_list)
        min_val = min(values_list)
        max_val = max(values_list)
        sum_squares = sum(v * v for v in values_list)
        
        yield (feature, {
            'count': count,
            'sum': total,
            'min': min_val,
            'max': max_val,
            'sum_sq': sum_squares
        })
    
    def reducer(self, feature, stats_dicts):
        """
        Reducer: Aggregate all statistics
        
        Input: (feature_name, [{stats1}, {stats2}, ...])
        Output: (feature_name, complete_statistics)
        """
        # Aggregate statistics from all mappers
        total_count = 0
        total_sum = 0
        global_min = float('inf')
        global_max = float('-inf')
        total_sum_sq = 0
        
        for stats in stats_dicts:
            total_count += stats['count']
            total_sum += stats['sum']
            global_min = min(global_min, stats['min'])
            global_max = max(global_max, stats['max'])
            total_sum_sq += stats['sum_sq']
        
        # Calculate final statistics
        mean = total_sum / total_count if total_count > 0 else 0
        
        # Variance: E[X^2] - E[X]^2
        variance = (total_sum_sq / total_count - mean * mean) if total_count > 0 else 0
        std_dev = variance ** 0.5
        
        result = {
            'count': total_count,
            'min': global_min,
            'max': global_max,
            'mean': round(mean, 4),
            'std': round(std_dev, 4),
            'variance': round(variance, 4),
            'range': round(global_max - global_min, 4)
        }
        
        yield (feature, json.dumps(result))
    
    def steps(self):
        """Define MapReduce steps with combiner"""
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    MRFeatureSummary.run()
