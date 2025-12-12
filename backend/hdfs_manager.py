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
        # Ensure directory structure exists
        self._initialize_directory_structure()
    
    def _initialize_directory_structure(self):
        """Create standard HDFS directory structure"""
        for dir_name, dir_path in HDFS_DIRS.items():
            self.mkdir(dir_path)
    
    # ==================== COMMAND EXECUTION ====================
    
    def run_command(self, args, capture_output=True):
        """
        Execute HDFS command with better error handling
        
        Args:
            args: List of command arguments
            capture_output: Whether to capture and return output
        
        Returns:
            tuple: (success, output/error_message)
        """
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
            return False, error_msg
        except Exception as e:
            return False, str(e)
    
    def run_docker_cp(self, src, dest):
        """Copy files to/from Docker container"""
        try:
            cmd = f'docker cp "{src}" "{dest}"'
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            return True, ""
        except subprocess.CalledProcessError as e:
            return False, e.output.decode('utf-8') if e.output else str(e)
    
    def run_docker_exec(self, cmd_str):
        """Execute command inside Docker container"""
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
    
    def create_directory(self, path):
        """Alias for mkdir with validation"""
        if not path.startswith("/"):
            return False, "HDFS paths must start with /"
        return self.mkdir(path)
    
    def list_files(self, path=HDFS_ROOT, recursive=False):
        """
        List files in HDFS directory
        
        Args:
            path: HDFS path to list
            recursive: Whether to list recursively
        
        Returns:
            list: File information dictionaries
        """
        args = ["-ls"]
        if recursive:
            args.append("-R")
        args.append(path)
        
        success, output = self.run_command(args)
        if not success:
            return []
        
        files = []
        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 8 and (line.startswith('d') or line.startswith('-')):
                # Parse ls output: permissions replication owner group size date time path
                is_dir = parts[0].startswith('d')
                files.append({
                    "type": "directory" if is_dir else "file",
                    "permission": parts[0],
                    "replication": parts[1] if not is_dir else "-",
                    "owner": parts[2],
                    "group": parts[3],
                    "size": parts[4],
                    "date": f"{parts[5]} {parts[6]}",
                    "name": parts[7] if len(parts) >= 8 else parts[-1],
                    "path": parts[7] if len(parts) >= 8 else parts[-1]
                })
        
        return files
    
    def list_directory_recursive(self, path=HDFS_ROOT):
        """List directory contents recursively"""
        return self.list_files(path, recursive=True)
    
    def get_directory_size(self, path):
        """
        Calculate total size of directory
        
        Returns:
            tuple: (success, size_in_bytes or error_message)
        """
        success, output = self.run_command(["-du", "-s", path])
        if not success:
            return False, output
        
        try:
            # Output format: "size path"
            size = output.split()[0]
            return True, int(size)
        except:
            return False, "Failed to parse directory size"
    
    def delete_directory(self, path, force=False):
        """
        Delete directory from HDFS
        
        Args:
            path: Directory path to delete
            force: Skip confirmation (use with caution)
        
        Returns:
            tuple: (success, message)
        """
        if not force and path in [HDFS_ROOT, HDFS_DIRS['processed']]:
            return False, "Cannot delete critical directories without force=True"
        
        return self.delete_file(path)  # -rm -r works for both
    
    # ==================== FILE OPERATIONS ====================
    
    def upload_file(self, local_path, hdfs_path=None, overwrite=True):
        """
        Upload file to HDFS with improved handling
        
        Args:
            local_path: Local file path
            hdfs_path: Destination HDFS path (auto-generated if None)
            overwrite: Whether to overwrite existing file
        
        Returns:
            tuple: (success, message)
        """
        if not os.path.exists(local_path):
            return False, f"Local file not found: {local_path}"
        
        if hdfs_path is None:
            filename = os.path.basename(local_path)
            hdfs_path = f"{HDFS_ROOT}/{filename}"
        
        # Remove existing file if overwrite
        if overwrite:
            self.run_command(["-rm", hdfs_path])
        
        if USE_DOCKER:
            # Docker workflow: copy to container, then put to HDFS
            file_basename = os.path.basename(local_path)
            tmp_path = f"/tmp/{file_basename}"
            
            # Step 1: Copy to container
            success, msg = self.run_docker_cp(local_path, f"{NAMENODE_CONTAINER}:{tmp_path}")
            if not success:
                return False, f"Docker copy failed: {msg}"
            
            # Step 2: Put to HDFS
            success, msg = self.run_command(["-put", tmp_path, hdfs_path])
            
            # Step 3: Cleanup
            self.run_docker_exec(f"rm -f {tmp_path}")
            
            if success:
                # Verify upload
                if self.file_exists(hdfs_path):
                    return True, f"Successfully uploaded to {hdfs_path}"
                else:
                    return False, "Upload completed but file not found in HDFS"
            return success, msg
        else:
            # Native HDFS
            return self.run_command(["-put", local_path, hdfs_path])
    
    def download_file(self, hdfs_path, local_path):
        """
        Download file from HDFS
        
        Args:
            hdfs_path: HDFS file path
            local_path: Local destination path
        
        Returns:
            tuple: (success, message)
        """
        # Ensure local directory exists
        local_dir = os.path.dirname(local_path)
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        # Remove existing local file
        if os.path.exists(local_path):
            os.remove(local_path)
        
        if USE_DOCKER:
            # Docker workflow: get to container tmp, then copy to host
            file_basename = os.path.basename(hdfs_path)
            tmp_path = f"/tmp/{file_basename}_{os.urandom(4).hex()}"
            
            # Step 1: Cleanup any existing tmp file
            self.run_docker_exec(f"rm -f {tmp_path}")
            
            # Step 2: Get from HDFS to container
            success, msg = self.run_command(["-get", hdfs_path, tmp_path])
            if not success:
                return False, f"HDFS get failed: {msg}"
            
            # Step 3: Copy from container to host
            success, msg = self.run_docker_cp(f"{NAMENODE_CONTAINER}:{tmp_path}", local_path)
            
            # Step 4: Cleanup
            self.run_docker_exec(f"rm -f {tmp_path}")
            
            if success and os.path.exists(local_path):
                return True, f"Downloaded to {local_path}"
            return False, "Download failed or file not created locally"
        else:
            # Native HDFS
            return self.run_command(["-get", hdfs_path, local_path])
    
    def download_batch(self, hdfs_paths, local_dir):
        """
        Download multiple files from HDFS
        
        Args:
            hdfs_paths: List of HDFS file paths
            local_dir: Local directory for downloads
        
        Returns:
            dict: Results for each file
        """
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        results = {}
        for hdfs_path in hdfs_paths:
            filename = os.path.basename(hdfs_path)
            local_path = os.path.join(local_dir, filename)
            success, msg = self.download_file(hdfs_path, local_path)
            results[hdfs_path] = {"success": success, "message": msg}
        
        return results
    
    def delete_file(self, hdfs_path):
        """Delete file or directory from HDFS"""
        return self.run_command(["-rm", "-r", hdfs_path])
    
    def file_exists(self, hdfs_path):
        """
        Check if file exists in HDFS
        
        Returns:
            bool: True if file exists
        """
        success, output = self.run_command(["-test", "-e", hdfs_path])
        return success
    
    def get_file_metadata(self, hdfs_path):
        """
        Get detailed metadata for a file
        
        Returns:
            dict: File metadata
        """
        success, output = self.run_command(["-stat", 
            "%n,%b,%y,%r,%u,%g", hdfs_path])
        
        if not success:
            return None
        
        try:
            parts = output.strip().split(',')
            return {
                "name": parts[0],
                "size_bytes": int(parts[1]),
                "modification_time": parts[2],
                "replication": parts[3],
                "owner": parts[4],
                "group": parts[5]
            }
        except:
            return None
    
    def cat_file(self, hdfs_path, head_bytes=None):
        """
        Read file content from HDFS
        
        Args:
            hdfs_path: HDFS file path
            head_bytes: Number of bytes to read (None for all)
        
        Returns:
            tuple: (success, content)
        """
        success, output = self.run_command(["-cat", hdfs_path])
        
        if success and head_bytes:
            return True, output[:head_bytes]
        return success, output
    
    def tail_file(self, hdfs_path, lines=10):
        """Get last N lines of file"""
        success, output = self.run_command(["-tail", hdfs_path])
        if success:
            all_lines = output.splitlines()
            return True, "\n".join(all_lines[-lines:])
        return success, output
    
    # ==================== ADVANCED OPERATIONS ====================
    
    def upload_with_progress(self, local_path, hdfs_path=None):
        """
        Upload file with basic progress indication
        For large files, returns file size info
        """
        if not os.path.exists(local_path):
            return False, "File not found", 0
        
        file_size = os.path.getsize(local_path)
        success, msg = self.upload_file(local_path, hdfs_path)
        
        return success, msg, file_size
    
    def upload_and_clean_file(self, local_path, hdfs_path=None):
        """
        Clean CMAPSS file (convert to CSV) and upload to HDFS
        Maintained for backward compatibility
        """
        try:
            from backend.data_cleaner import clean_cmapss_file
            import tempfile
            
            # Create temp file for cleaned CSV
            fd, temp_csv_path = tempfile.mkstemp(suffix=".csv")
            os.close(fd)
            
            # Clean the file
            success, msg = clean_cmapss_file(local_path, temp_csv_path)
            if not success:
                os.remove(temp_csv_path)
                return False, f"Cleaning failed: {msg}"
            
            # Determine HDFS path
            if hdfs_path is None:
                filename = os.path.basename(local_path)
                filename = os.path.splitext(filename)[0] + ".csv"
                hdfs_path = f"{HDFS_DIRS['processed']}/{filename}"
            
            # Upload
            success, msg = self.upload_file(temp_csv_path, hdfs_path)
            
            # Cleanup
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
            
            return success, msg
            
        except Exception as e:
            if 'temp_csv_path' in locals() and os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
            return False, f"Upload & clean failed: {str(e)}"
    
    def get_storage_summary(self):
        """
        Get HDFS storage usage summary
        
        Returns:
            dict: Storage statistics
        """
        summary = {}
        
        # Get total size of processed data
        for dir_type in ['train', 'test', 'rul']:
            dir_path = HDFS_DIRS.get(dir_type)
            if dir_path:
                success, size = self.get_directory_size(dir_path)
                summary[dir_type] = size if success else 0
        
        # Get total
        success, total = self.get_directory_size(HDFS_ROOT)
        summary['total'] = total if success else 0
        
        return summary
    
    def validate_upload(self, hdfs_path, expected_size=None):
        """
        Validate that file was uploaded correctly
        
        Args:
            hdfs_path: HDFS file path
            expected_size: Expected file size in bytes
        
        Returns:
            tuple: (is_valid, message)
        """
        if not self.file_exists(hdfs_path):
            return False, "File does not exist in HDFS"
        
        if expected_size:
            metadata = self.get_file_metadata(hdfs_path)
            if metadata and metadata.get('size_bytes') != expected_size:
                return False, f"Size mismatch: expected {expected_size}, got {metadata.get('size_bytes')}"
        
        return True, "Validation successful"


if __name__ == "__main__":
    # Test HDFS Manager
    hm = HDFSManager()
    print("HDFS Manager Initialized")
    
    # Test basic operations
    print("\n=== Testing HDFS Operations ===")
    
    # List root directory
    print(f"\nListing {HDFS_ROOT}:")
    files = hm.list_files(HDFS_ROOT)
    for f in files[:10]:  # Show first 10
        print(f"  {f['type']:10} {f['size']:>15} {f['name']}")
    
    # Get storage summary
    print("\n=== Storage Summary ===")
    summary = hm.get_storage_summary()
    for key, size in summary.items():
        print(f"  {key}: {size:,} bytes")
