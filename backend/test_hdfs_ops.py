from backend.hdfs_manager import HDFSManager
import os

def test_hdfs_operations():
    hm = HDFSManager()
    print("--- Testing HDFS File Management ---")

    # 1. Create Directory
    target_dir = "/bda_project/test_demo"
    print(f"\n[1] Creating Directory: {target_dir}")
    success, msg = hm.mkdir(target_dir)
    print(f"Result: {success} | {msg}")

    # 2. Add File (Upload)
    # Create a dummy local file
    local_file = "test_hdfs_upload.txt"
    with open(local_file, "w") as f:
        f.write("Hello HDFS! This is a test file.")
    
    hdfs_file = f"{target_dir}/uploaded_test.txt"
    print(f"\n[2] Adding File: {local_file} -> {hdfs_file}")
    success, msg = hm.upload_file(local_file, hdfs_file)
    print(f"Result: {success} | {msg}")

    # 3. Retrieve File (List/Cat)
    print(f"\n[3] Retrieving File Content: {hdfs_file}")
    success, content = hm.cat_file(hdfs_file)
    print(f"Content: {content}")

    # 4. List Directory
    print(f"\n[4] Listing Directory: {target_dir}")
    files = hm.list_files(target_dir)
    for f in files:
        print(f" - {f['name']} ({f['size']} bytes)")

    # 5. Delete File/Directory
    print(f"\n[5] Deleting Directory: {target_dir}")
    success, msg = hm.delete_file(target_dir)
    print(f"Result: {success} | {msg}")
    
    # Cleanup local
    if os.path.exists(local_file):
        os.remove(local_file)

if __name__ == "__main__":
    test_hdfs_operations()
