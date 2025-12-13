
import subprocess
import os

from backend.config import USE_DOCKER, NAMENODE_CONTAINER

class MapReduceManager:
    def __init__(self):
        self.jobs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "mapreduce_jobs")

    def run_job(self, job_script, input_file, runner="hadoop"):
        """
        Runs a MapReduce job on Hadoop YARN cluster via Docker.
        All jobs execute on the distributed YARN cluster.
        
        Args:
            job_script: Name of the MapReduce script to run
            input_file: HDFS path to input file
            runner: Execution mode (always 'hadoop' for YARN cluster)
        
        Returns:
            tuple: (success: bool, output: str)
        """
        if not USE_DOCKER:
            return False, "Docker is required for YARN execution. Please enable USE_DOCKER in config.py"
        
        script_path = os.path.join(self.jobs_dir, job_script)
        
        try:
            # 1. Copy script and config to container
            container_script_path = f"/tmp/{job_script}"
            config_path = os.path.join(self.jobs_dir, "mrjob.conf")
            container_config_path = "/tmp/mrjob.conf"

            # Copy script
            copy_cmd = ["docker", "cp", script_path, f"{NAMENODE_CONTAINER}:{container_script_path}"]
            subprocess.run(copy_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Copy config
            if os.path.exists(config_path):
                copy_conf_cmd = ["docker", "cp", config_path, f"{NAMENODE_CONTAINER}:{container_config_path}"]
                subprocess.run(copy_conf_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # 2. Prepare HDFS input path with proper scheme
            input_file = input_file.strip()
            final_input = input_file
            
            if not input_file.startswith("hdfs://"):
                # Ensure absolute path behavior
                if input_file.startswith("/"):
                    final_input = f"hdfs://namenode:9000{input_file}"
                else:
                    final_input = f"hdfs://namenode:9000/{input_file}"

            # 3. Build and execute MapReduce command on YARN
            cmd = ["docker", "exec", NAMENODE_CONTAINER, "python3", container_script_path, "-r", "hadoop"]
            
            if os.path.exists(config_path):
                cmd.extend(["--conf-path", container_config_path])
            
            cmd.append(final_input)
            
            # Execute on YARN cluster
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                return True, stdout
            else:
                return False, f"YARN Job Failed (Exit {process.returncode}):\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
                
        except Exception as e:
            return False, f"MapReduce Job Error: {str(e)}"
