#!/usr/bin/env python3
"""
Simple test script to verify MapReduce YARN execution
Tests the cycle_counter job on a small HDFS dataset
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.mapreduce_manager import MapReduceManager

def test_yarn_execution():
    print("=" * 60)
    print("YARN MapReduce Test Script")
    print("=" * 60)
    
    # Initialize MapReduce manager
    mrm = MapReduceManager()
    
    # Test with cycle counter job
    job_script = "mr_cycle_counter.py"
    input_file = "/bda_project/uploads/train_FD001.txt"
    
    print(f"\nJob Script: {job_script}")
    print(f"Input File: {input_file}")
    print(f"Runner: hadoop (YARN cluster)")
    print("\nStarting job execution...")
    print("-" * 60)
    
    # Run the job
    success, output = mrm.run_job(job_script, input_file, runner="hadoop")
    
    print("\n" + "=" * 60)
    if success:
        print("✅ JOB COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nJob Output:")
        print(output)
    else:
        print("❌ JOB FAILED")
        print("=" * 60)
        print("\nError Details:")
        print(output)
        return False
    
    print("\n" + "=" * 60)
    print("Test completed!")
    print("=" * 60)
    return True

if __name__ == "__main__":
    result = test_yarn_execution()
    sys.exit(0 if result else 1)
