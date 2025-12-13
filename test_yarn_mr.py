#!/usr/bin/env python3
"""
Simple test script to verify YARN MapReduce setup
"""
import subprocess
import sys
import os

def test_yarn_mapreduce():
    """Test if MapReduce job runs successfully with YARN"""
    
    # Copy script to container
    script_path = "mapreduce_jobs/mr_sensor_stats.py"
    config_path = "mapreduce_jobs/mrjob.conf"
    
    container_script = "/tmp/mr_sensor_stats.py"
    container_config = "/tmp/mrjob.conf"
    
    print("1. Copying files to namenode container...")
    subprocess.run(["docker", "cp", script_path, f"namenode:{container_script}"], check=True)
    subprocess.run(["docker", "cp", config_path, f"namenode:{container_config}"], check=True)
    
    print("2. Running MapReduce job with YARN...")
    cmd = [
        "docker", "exec", "namenode", 
        "python3", container_script,
        "-r", "hadoop",
        "--conf-path", container_config,
        "hdfs://namenode:9000/bda_project/uploads/train_FD001.txt"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print("\n=== STDOUT ===")
    print(result.stdout)
    
    if result.stderr:
        print("\n=== STDERR ===")
        print(result.stderr)
    
    if result.returncode == 0:
        print("\n✅ SUCCESS: MapReduce job completed with YARN!")
        return True
    else:
        print(f"\n❌ FAILED: Exit code {result.returncode}")
        return False

if __name__ == "__main__":
    success = test_yarn_mapreduce()
    sys.exit(0 if success else 1)
