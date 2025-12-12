"""
Enhanced HDFS Manager for CMAPSS Data Management
Provides comprehensive HDFS operations with improved error handling and validation
"""

import subprocess
import os
import json
from backend.config import (
    get_hdfs_cmd_prefix, HDFS_ROOT, HDFS_DIRS, USE_DOCKER, 
    NAMENODE_CONTAINER, validate_dataset_id
)

class HDFSManager:
    """Manages all HDFS operations with enhanced functionality"""
    
    def __init__(self):
        self.cmd_prefix = get_hdfs_cmd_prefix()
        # Ensure directory structure exists (lazy loaded usually, but good to init)
        # We don't want to crash init if HDFS is down, so we wrap this
        try:
            self._initialize_directory_structure()
        except:
            pass
    
    def _initialize_directory_structure(self):
        """Create standard HDFS directory structure"""
        for dir_name, dir_path in HDFS_DIRS.items():
            self.mkdir(dir_path)
    
    # ==================== COMMAND EXECUTION ====================
    
    def run_command(self, args, capture_output=True):
        full_cmd = self.cmd_prefix + args
        try:
            cmd_str = " ".join(full_cmd)
            if capture_output:
                result = subprocess.check_output(
                    cmd_str, shell=True, stderr=subprocess.STDOUT, text=True
                )
                return True, result
            else:
                subprocess.check_call(cmd_str, shell=True)
                return True, "Command executed successfully"
        except subprocess.CalledProcessError as e:
            error_msg = e.output if hasattr(e, 'output') and e.output else str(e)
            if "Connection refused" in error_msg:
                return False, "Connection refused. Please ensure HDFS Docker containers are running."
            return False, error_msg
        except Exception as e:
            return False, str(e)
    
    def run_docker_cp(self, src, dest):
        try:
            cmd = f'docker cp "{src}" "{dest}"'
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            return True, ""
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)
    
    def run_docker_exec(self, cmd_str):
        full_cmd = f"docker exec {NAMENODE_CONTAINER} {cmd_str}"
        try:
            result = subprocess.check_output(full_cmd, shell=True, stderr=subprocess.STDOUT)
            return True, result.decode('utf-8') if result else ""
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)
    
    # ==================== DIRECTORY OPERATIONS ====================
    
    def mkdir(self, path):
        """Create directory (with parents) in HDFS"""
        return self.run_command(["-mkdir", "-p", path])
    
    def list_files(self, path=HDFS_ROOT, recursive=False):
        args = ["-ls"]
        if recursive:
            args.append("-R")
        args.append(path)
        
        success, output = self.run_command(args)
        if not success:
            return []
        
        files = []
        for line in output.splitlines():
            # HDFS ls output format: permissions replication owner group size date time path
            # Example: -rw-r--r--   1 root supergroup       1366 2023-10-27 12:00 /path/to/file
            parts = line.split()
            if len(parts) >= 8:
                # Check for directory flag
                is_dir = line.startswith('d')
                
                # Basic parsing trying to handle standard hadoop ls output
                try:
                    # We expect at least 8 columns. The last one is the path.
                    path = parts[-1]
                    name = os.path.basename(path)
                    if not name: # handling root /
                         name = path
                    
                    # Extract other metadata
                    permission = parts[0]
                    owner = parts[2]
                    size = parts[4]
                    date = f"{parts[5]} {parts[6]}"
                    
                    files.append({
                        "type": "directory" if is_dir else "file",
                        "permission": permission,
                        "owner": owner,
                        "size": size,
                        "date": date,
                        "name": name,
                        "path": path
                    })
                except Exception:
                    # Skip malformed lines silently or log if needed
                    continue
        return files
    
    def delete_file(self, hdfs_path):
        """Delete file or directory from HDFS"""
        return self.run_command(["-rm", "-r", hdfs_path])
    
    # ==================== FILE OPERATIONS ====================
    
    def upload_file(self, local_path, hdfs_path=None, overwrite=True):
        if not os.path.exists(local_path):
            return False, f"Local file not found: {local_path}"
        
        if hdfs_path is None:
            filename = os.path.basename(local_path)
            hdfs_path = f"{HDFS_ROOT}/{filename}"
        
        if overwrite:
            self.run_command(["-rm", hdfs_path])
        
        if USE_DOCKER:
            file_basename = os.path.basename(local_path)
            tmp_path = f"/tmp/{file_basename}"
            
            try:
                success, msg = self.run_docker_cp(local_path, f"{NAMENODE_CONTAINER}:{tmp_path}")
                if not success: return False, f"Docker copy failed: {msg}"
                
                success, msg = self.run_command(["-put", tmp_path, hdfs_path])
                return success, msg
            finally:
                # Ensure we clean up the temp file inside the container
                self.run_docker_exec(f"rm -f {tmp_path}")
        else:
            return self.run_command(["-put", local_path, hdfs_path])
    
    def download_file(self, hdfs_path, local_path):
        local_dir = os.path.dirname(local_path)
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        if os.path.exists(local_path):
            os.remove(local_path)
        
        if USE_DOCKER:
            file_basename = os.path.basename(hdfs_path)
            # Use random suffix to avoid collision
            import random
            tmp_path = f"/tmp/{file_basename}_{random.randint(1000,9999)}"
            
            # Get HDFS -> Container
            success, msg = self.run_command(["-get", hdfs_path, tmp_path])
            if not success: return False, f"HDFS get failed: {msg}"
            
            # Container -> Host
            success, msg = self.run_docker_cp(f"{NAMENODE_CONTAINER}:{tmp_path}", local_path)
            self.run_docker_exec(f"rm -f {tmp_path}")
            
            if success and os.path.exists(local_path):
                return True, f"Downloaded to {local_path}"
            return False, "Download failed or file not created locally"
        else:
            return self.run_command(["-get", hdfs_path, local_path])

    def cat_file(self, hdfs_path, head_bytes=None):
        # Improved cat that handles connection errors gracefully
        success, output = self.run_command(["-cat", hdfs_path])
        if not success:
             return False, output
             
        if head_bytes and len(output) > head_bytes:
            return True, output[:head_bytes] + "\n... (truncated)"
        return True, output
    
    def get_storage_summary(self):
        summary = {}
        for dir_type in ['train', 'test', 'rul']:
            dir_path = HDFS_DIRS.get(dir_type)
            if dir_path:
                success, output = self.run_command(["-du", "-s", dir_path])
                if success:
                    try:
                        summary[dir_type] = int(output.split()[0])
                    except:
                        summary[dir_type] = 0
                else:
                    summary[dir_type] = 0
        
        success, output = self.run_command(["-du", "-s", HDFS_ROOT])
        if success:
             try:
                 summary['total'] = int(output.split()[0])
             except:
                 summary['total'] = 0
        else:
            summary['total'] = 0
            
        return summary
    
    def file_exists(self, hdfs_path):
        success, output = self.run_command(["-test", "-e", hdfs_path])
        return success

if __name__ == "__main__":
    hm = HDFSManager()
    print("HDFS Manager Initialized")
