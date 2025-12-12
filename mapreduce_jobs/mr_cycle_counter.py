#!/usr/bin/env python
"""
MapReduce Job: Cycle Counter
Counts total cycles and provides statistics per engine unit
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
import re


class MRCycleCounter(MRJob):
    """
    MapReduce job to count cycles per engine unit
    Useful for identifying engines with unusual operational lifespans
    """
    
    def mapper(self, _, line):
        """
        Map phase: Extract unit number and cycle count
        
        Input: CSV line with engine data
        Output: (unit_number, cycle_count)
        """
        # Skip empty lines
        if not line or not line.strip():
            return
        
        try:
            # Parse CSV (assuming comma-separated)
            parts = line.strip().split(',')
            
            if len(parts) < 2:
                return
            
            # Extract unit_number and time_cycles
            unit_number = int(float(parts[0]))
            time_cycles = int(float(parts[1]))
            
            # Emit: unit_number -> cycle
            yield (unit_number, time_cycles)
            
        except (ValueError, IndexError):
            # Skip malformed lines
            pass
    
    def reducer_max_cycles(self, unit_number, cycles):
        """
        Reduce phase: Find maximum cycle for each unit
        
        Input: (unit_number, [cycle1, cycle2, ...])
        Output: (unit_number, max_cycle)
        """
        max_cycle = max(cycles)
        yield (unit_number, max_cycle)
    
    def reducer_count_distribution(self, unit_number, max_cycles):
        """
        Second reduce: Calculate distribution statistics
        
        Input: (unit_number, max_cycle)
        Output: Statistics
        """
        cycles_list = list(max_cycles)
        
        # This will have single value per unit from previous reducer
        for cycle in cycles_list:
            yield ("UNIT_STATS", {
                "unit": unit_number,
                "max_cycles": cycle
            })
    
    def steps(self):
        """Define MapReduce steps"""
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer_max_cycles
            )
        ]


if __name__ == '__main__':
    MRCycleCounter.run()
