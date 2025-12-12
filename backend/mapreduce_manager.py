import subprocess
import os
import sys

class MapReduceManager:
    def __init__(self):
        self.jobs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "mapreduce_jobs")

    def run_job(self, job_script, input_file, runner="inline"):
        """
        Runs a MapReduce job.
        runner: 'inline' (local simulation) or 'hadoop' (actual cluster)
        """
        script_path = os.path.join(self.jobs_dir, job_script)
        
        # Command construction
        # python job_script.py -r runner input_file
        # Note: input_file must be absolute path
        
        cmd = [sys.executable, script_path, "-r", runner, input_file]

        try:
            # Run command
            # Capture both stdout (results) and stderr (logs)
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                return True, stdout
            else:
                return False, f"STDOUT: {stdout}\nSTDERR: {stderr}"
        except Exception as e:
            return False, str(e)

