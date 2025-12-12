
import subprocess
import os
import sys
import shutil

class MapReduceManager:
    def __init__(self):
        self.jobs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "mapreduce_jobs")

    def run_job(self, job_script, input_file, runner="inline"):
        """
        Runs a MapReduce job.
        runner: 'inline' (local simulation) or 'hadoop' (actual cluster)
        """
        script_path = os.path.join(self.jobs_dir, job_script)
        
        # Windows check for hadoop runner
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
