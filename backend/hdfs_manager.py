import subprocess
import os
from backend.config import get_hdfs_cmd_prefix, HDFS_ROOT, USE_DOCKER, NAMENODE_CONTAINER

class HDFSManager:
    def __init__(self):
        self.cmd_prefix = get_hdfs_cmd_prefix()
        # Ensure root directory exists
        self.mkdir(HDFS_ROOT)

    def run_command(self, args):
        """Helper to run shell commands"""
        # If args is a string, split it, but usually we pass list. 
        # HDFS commands might be complex.
        
        full_cmd = self.cmd_prefix + args
        
        # Join for Windows execution compatibility if needed, 
        # but for docker commands, keeping as list passed to subprocess is often cleaner if shell=False.
        # However, existing code uses shell=True and join. Let's maintain pattern for regular commands.
        
        try:
            cmd_str = " ".join(full_cmd)
            # print(f"Executing: {cmd_str}") # Debug
            result = subprocess.check_output(cmd_str, shell=True, stderr=subprocess.STDOUT)
            return True, result.decode('utf-8')
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)

    def run_docker_cp(self, src, dest):
        """Helper to run docker cp"""
        try:
            cmd = f"docker cp \"{src}\" \"{dest}\""
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            return True, ""
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8')

    def run_docker_exec(self, cmd_str):
        """Helper to run command inside docker purely"""
        # cmd_str e.g. "rm /tmp/file"
        full_cmd = f"docker exec {NAMENODE_CONTAINER} {cmd_str}"
        try:
            subprocess.check_output(full_cmd, shell=True, stderr=subprocess.STDOUT)
            return True, ""
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8')

    def list_files(self, path=HDFS_ROOT):
        success, output = self.run_command(["-ls", path])
        if not success:
            return []
        
        files = []
        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 8:
                # Basic parsing logic for `hdfs dfs -ls` output
                # -rw-r--r--   1 user supergroup       1366 2023-10-10 10:00 /path/to/file
                permission = parts[0]
                owner = parts[2]
                size = parts[4]
                name = parts[-1]
                if name == path: continue # Skip the dir itself
                files.append({
                    "name": name,
                    "size": size,
                    "owner": owner,
                    "permission": permission
                })
        return files

    def mkdir(self, path):
        return self.run_command(["-mkdir", "-p", path])

    def upload_file(self, local_path, hdfs_path=None):
        if hdfs_path is None:
            filename = os.path.basename(local_path)
            hdfs_path = f"{HDFS_ROOT}/{filename}"
        
        # Cleanup destination first (force overwrite logic)
        self.run_command(["-rm", hdfs_path])

        if USE_DOCKER:
            # 1. Copy to container /tmp
            file_basename = os.path.basename(local_path)
            tmp_path = f"/tmp/{file_basename}"
            
            # docker cp local_path namenode:/tmp/filename
            success, msg = self.run_docker_cp(local_path, f"{NAMENODE_CONTAINER}:{tmp_path}")
            if not success: return False, f"Docker CP Failed: {msg}"
            
            # 2. HDFS put from /tmp
            # Note: run_command prefixes "docker exec namenode hdfs dfs"
            # So we just pass ["-put", tmp_path, hdfs_path]
            success, msg = self.run_command(["-put", tmp_path, hdfs_path])
            
            # 3. Cleanup tmp
            self.run_docker_exec(f"rm {tmp_path}")
            
            return success, msg
        else:
            return self.run_command(["-put", local_path, hdfs_path])

    def download_file(self, hdfs_path, local_path):
        if USE_DOCKER:
            # 1. HDFS get to /tmp inside container
            file_basename = os.path.basename(hdfs_path)
            tmp_path = f"/tmp/{file_basename}_{os.urandom(4).hex()}" # Randomize to avoid collisions
            
            # Cleanup tmp first just in case
            self.run_docker_exec(f"rm {tmp_path}")
            
            success, msg = self.run_command(["-get", hdfs_path, tmp_path])
            if not success: return False, f"HDFS Get Failed: {msg}"
            
            # 2. Docker cp from container to host
            success, msg = self.run_docker_cp(f"{NAMENODE_CONTAINER}:{tmp_path}", local_path)
            
            # 3. Cleanup tmp
            self.run_docker_exec(f"rm {tmp_path}")
            
            return success, msg
        else:
            return self.run_command(["-get", hdfs_path, local_path])

    def delete_file(self, hdfs_path):
        return self.run_command(["-rm", "-r", hdfs_path])

    def cat_file(self, hdfs_path, head_bytes=1000):
        # Using -cat | head isn't easy via single subprocess call without shell pipes
        # Just use -cat and truncate in python
        success, output = self.run_command(["-cat", hdfs_path])
        if success:
            return True, output[:head_bytes]
        return False, output

    def upload_and_clean_file(self, local_path, hdfs_path=None):
        """
        Cleans the local file (converts to CSV) and then uploads to HDFS.
        Useful for raw CMAPSS files.
        """
        try:
            from backend.data_cleaner import clean_cmapss_file
            import tempfile
            
            # Create a temp file for the cleaned CSV
            fd, temp_csv_path = tempfile.mkstemp(suffix=".csv")
            os.close(fd)
            
            # Clean the file
            success, msg = clean_cmapss_file(local_path, temp_csv_path)
            if not success:
                os.remove(temp_csv_path)
                return False, f"Cleaning Failed: {msg}"
            
            # Determine HDFS path
            if hdfs_path is None:
                filename = os.path.basename(local_path)
                # Change extension to .csv
                filename = os.path.splitext(filename)[0] + ".csv"
                hdfs_path = f"{HDFS_ROOT}/processed/{filename}"
            
            # Upload
            success, msg = self.upload_file(temp_csv_path, hdfs_path)
            
            # Cleanup temp
            os.remove(temp_csv_path)
            
            return success, msg
            
        except Exception as e:
            if 'temp_csv_path' in locals() and os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
            return False, f"Upload & Clean Failed: {str(e)}"

if __name__ == "__main__":
    hm = HDFSManager()
    print("HDFS Manager Initialized")
