from ftplib import FTP
import os

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

def upload_specific_file(ftp, local_file_path, remote_directory_path):
    if os.path.exists(local_file_path):
        remote_file_name = os.path.basename(local_file_path)
        ensure_directory_exists(ftp, remote_directory_path)
        ftp.cwd(remote_directory_path)
        with open(local_file_path, 'rb') as file:
            ftp.storbinary(f'STOR {remote_file_name}', file)
        print(f"File '{local_file_path}' has been uploaded to '{remote_directory_path}' on the server")
    else:
        raise FileNotFoundError(f"File '{local_file_path}' not found")

def list_files_on_server(ftp, path='/'):
    ftp.cwd(path)
    ftp.retrlines('LIST')

def upload_csv_ctrl_to_ftp_server(**kwargs):
    ftp_server = '34.124.138.144'
    username = 'airflow'
    password = 'airflow'
    ftp = connect_to_ftp(ftp_server, username, password)

    ti = kwargs['task_instance']
    dag_id = ti.dag_id

    output_filename_csv = ti.xcom_pull(dag_id={dag_id}, key='output_filename')
    print("output_filename :", output_filename_csv)
    print("dag_id :",dag_id)

    output_filename_ctrl = ti.xcom_pull(dag_id={dag_id},key='control_filename')
    print("control_filename :", output_filename_ctrl)
    print("dag_id :",dag_id)
    
    csv_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/ELK_{dag_id}/'
    ctrl_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/ELK_{dag_id}/'

    csv_local_file_path = f'/opt/airflow/output/batch_process/{output_filename_csv}'
    ctrl_local_file_path = f'/opt/airflow/output/control/{output_filename_ctrl}'

    upload_specific_file(ftp, csv_local_file_path, csv_remote_path)
    upload_specific_file(ftp, ctrl_local_file_path, ctrl_remote_path)

    list_files_on_server(ftp)

    ftp.quit()

if __name__ == "__main__":
    upload_csv_ctrl_to_ftp_server()
