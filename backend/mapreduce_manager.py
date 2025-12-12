
import subprocess
import os
import sys
import shutil

from backend.config import USE_DOCKER, NAMENODE_CONTAINER

class MapReduceManager:
    def __init__(self):
        self.jobs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "mapreduce_jobs")

    def run_job(self, job_script, input_file, runner="inline"):
        """
        Runs a MapReduce job.
        runner: 'inline' (local simulation) or 'hadoop' (actual cluster)
        """
        script_path = os.path.join(self.jobs_dir, job_script)
        
        # Dockerized Hadoop Execution
        if runner == "hadoop" and USE_DOCKER:
             try:
                 # 1. Copy script to container
                 container_script_path = f"/tmp/{job_script}"
                 copy_cmd = ["docker", "cp", script_path, f"{NAMENODE_CONTAINER}:{container_script_path}"]
                 subprocess.run(copy_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                 
                 # 2. Run inside container using the installed python3
                 # Note: standard mrjob on hadoop runner usually just needs the script + args
                 
                 # Fix: Explicitly use hdfs scheme to prevent mrjob from checking local filesystem
                 input_file = input_file.strip()
                 final_input = input_file
                 
                 if not input_file.startswith("hdfs://"):
                     # Ensure absolute path behavior if it looks like a path
                     if input_file.startswith("/"):
                         final_input = f"hdfs://namenode:9000{input_file}"
                     else:
                         # Assume paths without / are relative to /bda_project default or user home, 
                         # but for now let's just prepend slash to be safe if it looks like a filename
                         final_input = f"hdfs://namenode:9000/{input_file}"

                 cmd = ["docker", "exec", NAMENODE_CONTAINER, "python3", container_script_path, "-r", "hadoop", final_input]
                 
                 # Using a large buffer or just communicating relies on pipes
                 process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 stdout, stderr = process.communicate()
                 
                 if process.returncode == 0:
                     return True, stdout
                 else:
                     return False, f"Docker Execution Failed (Exit {process.returncode}):\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
             except Exception as e:
                 return False, f"Docker Job Error: {str(e)}"

        # Native Windows check (only if NOT using Docker)
        if runner == "hadoop" and os.name == 'nt':
             # Simple check if "hadoop" is in PATH
             if shutil.which("hadoop") is None:
                 # Fallback suggestion or error
                 return False, "Hadoop binary not found in PATH. Please install Hadoop or use 'inline' runner for local simulation."

        cmd = [sys.executable, script_path, "-r", runner, input_file]
        
        # Add basic python path to ensure imports work if needed
        env = os.environ.copy()
        if "PYTHONPATH" not in env:
             env["PYTHONPATH"] = os.getcwd()

        try:
            # Run command
            # Capture both stdout (results) and stderr (logs)
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                return True, stdout
            else:
                return False, f"Execution Failed (Exit Code {process.returncode})\n\nSTDERR:\n{stderr}\n\nSTDOUT:\n{stdout}"
        except Exception as e:
            return False, f"System Error: {str(e)}"
