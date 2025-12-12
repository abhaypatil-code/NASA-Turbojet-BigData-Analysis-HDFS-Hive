import subprocess
import os
from backend.config import get_hdfs_cmd_prefix, HDFS_ROOT

class HDFSManager:
    def __init__(self):
        self.cmd_prefix = get_hdfs_cmd_prefix()
        # Ensure root directory exists
        self.mkdir(HDFS_ROOT)

    def run_command(self, args):
        """Helper to run shell commands"""
        cmd = self.cmd_prefix + args
        try:
            # shell=True required for Windows sometimes, but list args is safer
            # using shell=True means cmd must be a string.
            # On Windows, subprocess with list args works if executable is in path.
            # If using 'hdfs' command which is often a bat file, shell=True might be needed.
            # We will try list format first.
            
            # For HDFS on Windows, often 'hdfs' is a .cmd file.
            cmd_str = " ".join(cmd) if os.name == 'nt' else cmd
            
            # Using shell=True for broader compatibility with environment variables
            result = subprocess.check_output(cmd_str, shell=True, stderr=subprocess.STDOUT)
            return True, result.decode('utf-8')
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)

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
        
        # force overwriting (-f) not always available in old hadoop, so rm first
        self.run_command(["-rm", hdfs_path])
        return self.run_command(["-put", local_path, hdfs_path])

    def download_file(self, hdfs_path, local_path):
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

if __name__ == "__main__":
    hm = HDFSManager()
    print("HDFS Manager Initialized")
