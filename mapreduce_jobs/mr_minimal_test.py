#!/usr/bin/env python3
"""
Minimal MapReduce test - word count style
"""
from mrjob.job import MRJob

class MRMinimalTest(MRJob):
    def mapper(self, _, line):
        """Simple mapper - just count lines"""
        yield ("total_lines", 1)
    
    def reducer(self, key, values):
        """Simple reducer - sum counts"""
        yield (key, sum(values))

if __name__ == '__main__':
    MRMinimalTest.run()
