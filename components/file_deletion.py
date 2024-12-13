import os
from datetime import datetime, timedelta
from components.database import delete_batch_state

# Constants
BASE_DIRS = ["/opt/airflow/data/batch", "/opt/airflow/data/ftps", "/opt/airflow/logs"]
RETENTION_DAYS = 14
FILE_TYPE = '.csv'

def delete_file(file_path):
    """Delete a file and print the result."""
    try:
        os.remove(file_path)
        print(f"Deleted file: {file_path}")
    except Exception as e:
        print(f"Error deleting file {file_path}: {e}")

def delete_empty_directory(directory_path):
    """Delete an empty directory and print the result."""
    if not os.listdir(directory_path):  # Check if the directory is empty
        try:
            os.rmdir(directory_path)
            print(f"Deleted empty directory: {directory_path}")
        except Exception as e:
            print(f"Error deleting directory {directory_path}: {e}")

def check_and_log_if_empty(directory):
    """Check if the specified directory is empty and log a message if it is."""
    if not os.listdir(directory):
        print(f"The directory {directory} is now empty.")

def delete_old_batch_files():
    """
    Automatically delete files older than RETENTION_DAYS
    and clean up empty directories in BASE_DIRS.
    """
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)

    for base_dir in BASE_DIRS:
        if not os.path.exists(base_dir):
            print(f"Base directory {base_dir} does not exist.")
            continue

        for root, dirs, files in os.walk(base_dir, topdown=False):
            for filename in files:
                file_path = os.path.join(root, filename)
                if os.path.isfile(file_path) and datetime.fromtimestamp(os.path.getmtime(file_path)) < cutoff_date:
                    delete_file(file_path)
                    if filename.endswith(FILE_TYPE):
                        delete_batch_state(filename)
                    
            for directory in dirs:
                delete_empty_directory(os.path.join(root, directory))

        check_and_log_if_empty(base_dir)