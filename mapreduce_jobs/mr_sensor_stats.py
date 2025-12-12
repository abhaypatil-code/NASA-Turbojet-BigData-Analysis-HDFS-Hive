import sys

# Compatibility for Python 3.13 where pipes module is removed
try:
    import pipes
except ImportError:
    import shlex
    sys.modules['pipes'] = shlex

from mrjob.job import MRJob
from mrjob.step import MRStep
import statistics

class MRSensorStats(MRJob):
    """
    Calculates Min, Max, and Average for Sensor 11 across all units.
    Sensor 11 is 'Static Pressure'
    Input: Space-separated text file (CMaps)
    """

    def mapper(self, _, line):
        # Format: Unit Time Op1 Op2 Op3 S1 ... S21
        try:
            parts = line.split()
            if len(parts) >= 26:
                # Sensor 11 is at index 5 + 10 = 15 (0-indexed)
                # S1 is index 5
                # S11 is index 15
                sensor_11_val = float(parts[15])
                yield "Sensor_11", sensor_11_val
        except Exception:
            pass

    def reducer(self, key, values):
        vals = list(values)
        if vals:
            yield key, {
                "min": min(vals),
                "max": max(vals),
                "avg": sum(vals) / len(vals),
                "count": len(vals)
            }

if __name__ == '__main__':
    MRSensorStats.run()
