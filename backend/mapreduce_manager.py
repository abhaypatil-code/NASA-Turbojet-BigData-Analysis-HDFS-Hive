import subprocess
import os
import sys

# We will use mrjob's runner capability or simple subprocess if mrjob is complicated to wrap programmatically
# For simplicity and robustness in this assignment context, we'll execute them as subprocesses.
# By default mrjob runs locally (inline). To run on Hadoop, we'd add `-r hadoop`.

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
        cmd = [sys.executable, script_path, "-r", runner]
        
        # If input_file is an absolute path, use it.
        # If it's a relative path/HDFS path, mrjob handles it differently.
        # For 'inline', input must be local file.
        # For 'hadoop', input can be HDFS path.
        
        cmd.append(input_file)

        try:
            # Run command
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                return True, stdout
            else:
                return False, stderr
        except Exception as e:
            return False, str(e)
