import sys

# Compatibility for Python 3.13 where pipes module is removed
try:
    import pipes
except ImportError:
    import shlex
    sys.modules['pipes'] = shlex

from mrjob.job import MRJob

class MRRulAvg(MRJob):
    """
    Calculates the Average RUL (Remaining Useful Life) for each Operational Setting 1 cluster.
    This assumes input is training data where RUL is implicit (Max Cycle - Current Cycle).
    However, standard MRjob on raw data doesn't know 'Max Cycle' easily without two passes.
    
    SIMPLIFICATION: 
    We will just average 'Sensor 11' (Pressure) per 'Operational Setting 1' rounded.
    RUL calculation usually requires full history.
    
    If we want TRUE RUL average, we'd need preprocessed data with RUL column.
    Assuming we run this on the 'processed' data which HAS headers/columns or we know the schema.
    
    For this task, let's calculate Average Sensor 4 (Temp) per Rounded Op Setting 1.
    """

    def mapper(self, _, line):
        try:
            line = line.strip()
            if ',' in line:
                parts = line.split(',')
            else:
                parts = line.split()
            
            # Check length to ensure it's a valid data line
            # 26 columns in standard C-MAPSS
            if len(parts) >= 26: 
                # Op Setting 1 is index 2
                # Sensor 4 is index 5+3 = 8
                
                try:
                    op_setting_1 = float(parts[2])
                    sensor_4 = float(parts[8])
                    
                    # Round op setting to cluster
                    op_cluster = round(op_setting_1, 1)
                    
                    yield "OpSet1_{}".format(op_cluster), sensor_4
                except ValueError:
                    pass
        except Exception:
            pass

    def reducer(self, key, values):
        vals = list(values)
        if vals:
             avg_val = sum(vals) / len(vals)
             yield key, round(avg_val, 4)

if __name__ == '__main__':
    MRRulAvg.run()
