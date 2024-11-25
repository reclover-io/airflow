import os
from datetime import datetime, timedelta

# Constants
BASE_DIR = "/opt/airflow/data/batch"
RETENTION_DAYS = 14


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
    and clean up empty directories in BASE_DIR.
    """
    if not os.path.exists(BASE_DIR):
        print(f"Base directory {BASE_DIR} does not exist.")
        return

    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)

    for root, dirs, files in os.walk(BASE_DIR, topdown=False):
        for filename in files:
            file_path = os.path.join(root, filename)
            if os.path.isfile(file_path) and datetime.fromtimestamp(os.path.getmtime(file_path)) < cutoff_date:
                delete_file(file_path)

        for directory in dirs:
            delete_empty_directory(os.path.join(root, directory))

    check_and_log_if_empty(BASE_DIR)


def delete_all_batch_files():
    """Delete all files and empty directories in BASE_DIR."""
    if not os.path.exists(BASE_DIR):
        print(f"Base directory {BASE_DIR} does not exist.")
        return

    for root, dirs, files in os.walk(BASE_DIR, topdown=False):
        for filename in files:
            delete_file(os.path.join(root, filename))

        for directory in dirs:
            delete_empty_directory(os.path.join(root, directory))

    check_and_log_if_empty(BASE_DIR)
