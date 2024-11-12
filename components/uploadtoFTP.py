from ftplib import FTP
import os
import glob
from datetime import datetime

def connect_to_ftp(server, username, password):
    ftp = FTP(server)
    ftp.login(user=username, passwd=password)
    return ftp

def ensure_directory_exists(ftp, path):
    dirs = path.strip('/').split('/')
    current_path = ''
    for directory in dirs:
        current_path += f'/{directory}'
        try:
            ftp.cwd(current_path)
        except:
            ftp.mkd(current_path)
            ftp.cwd(current_path)

def upload_latest_file(ftp, local_directory_path, remote_directory_path, file_extension):
    latest_file = max(glob.glob(os.path.join(local_directory_path, f'*{file_extension}')), key=os.path.getctime, default=None)
    if latest_file:
        remote_file_name = os.path.basename(latest_file)
        ensure_directory_exists(ftp, remote_directory_path)
        ftp.cwd(remote_directory_path)
        with open(latest_file, 'rb') as file:
            ftp.storbinary(f'STOR {remote_file_name}', file)
        print(f"File '{latest_file}' has been uploaded to '{remote_directory_path}' on the server")
    else:
        print(f"No {file_extension} file found in the specified directory")

def list_files_on_server(ftp, path='/'):
    ftp.cwd(path)
    ftp.retrlines('LIST')

def upload_csv_ctrl_to_ftp_server():
    ftp_server = '192.168.0.101'
    username = 'airflow'
    password = 'airflow'
    ftp = connect_to_ftp(ftp_server, username, password)
    csv_remote_path = '/csv_files/'
    ctrl_remote_path = '/control_files/'


    csv_directory_path = '/opt/airflow/output/batch_process'
    ctrl_directory_path = '/opt/airflow/output/control'

    upload_latest_file(ftp, csv_directory_path, csv_remote_path, '.csv')
    upload_latest_file(ftp, ctrl_directory_path, ctrl_remote_path, '.ctrl')

    list_files_on_server(ftp)

    ftp.quit()

if __name__ == "__main__":
    upload_csv_ctrl_to_ftp_server()
