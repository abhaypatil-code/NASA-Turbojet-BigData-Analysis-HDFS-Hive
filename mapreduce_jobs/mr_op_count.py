import sys

# Compatibility for Python 3.13 where pipes module is removed
try:
    import pipes
except ImportError:
    import shlex
    sys.modules['pipes'] = shlex

from mrjob.job import MRJob


class MROpCount(MRJob):
    """
    Counts the number of records per Unit Number.
    Input: CMaps text file
    """

    def mapper(self, _, line):
        try:
            line = line.strip()
            if ',' in line:
                parts = line.split(',')
            else:
                parts = line.split()
            
            if len(parts) > 0:
                unit_id = parts[0]
                yield f"Unit_{unit_id}", 1
        except Exception:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MROpCount.run()
