import os
from datetime import datetime, timedelta
from components.database import delete_batch_state

# Constants
BASE_DIRS = ["/opt/airflow/data/batch", "/opt/airflow/logs"]
RETENTION_DAYS = 14

def delete_file(file_paths):
    """Delete a list of files and print the result."""
    for file_path in file_paths:
        try:
            print(f"Deleted file: {file_path}")
            os.remove(file_path)
            folder_path = os.path.dirname(file_path)
            delete_empty_directory(folder_path)
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")

def delete_empty_directory(directory_path):
    """Delete an empty directory and print the result."""
    # ตรวจสอบว่า directory_path ไม่ใช่ BASE_DIR
    if directory_path not in BASE_DIRS:  
        if not os.listdir(directory_path):  # Check if the directory is empty
            try:
                os.rmdir(directory_path)
                print(f"Deleted empty directory: {directory_path}")
            except Exception as e:
                print(f"Error deleting directory {directory_path}: {e}")
    else:
        print(f"Skipping base directory: {directory_path}")

def check_and_log_if_empty(directory):
    """Check if the specified directory is empty and log a message if it is."""
    if not os.listdir(directory):
        print(f"The directory {directory} is now empty.")

def delete_old_batch_files():
    """
    Collect files older than RETENTION_DAYS, remove corresponding database entries,
    and then delete the files from the filesystem.
    """
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    file_path_list = []  # Store full file paths for deletion

    for base_dir in BASE_DIRS:
        if not os.path.exists(base_dir):
            print(f"Base directory {base_dir} does not exist.")
            continue

        # Collect files for deletion
        for root, dirs, files in os.walk(base_dir, topdown=False):
            for filename in files:
                file_path = os.path.join(root, filename)
                if os.path.isfile(file_path) and datetime.fromtimestamp(os.path.getmtime(file_path)) < cutoff_date:
                    file_path_list.append(file_path)

            for directory in dirs:
                delete_empty_directory(os.path.join(root, directory))

        check_and_log_if_empty(base_dir)

    delete_batch_state()
    delete_file(file_path_list)