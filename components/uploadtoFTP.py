from ftplib import FTP_TLS, error_perm, error_temp
import os
from airflow.exceptions import AirflowSkipException, AirflowException
from typing import Optional, Dict, List
from components.notifications import send_retry_notification
from components.database import (
    get_batch_state, 
    save_batch_state,
)


def connect_to_ftps(server, username, password):
    """
    Connect to FTPS server using FTP_TLS.
    """
    ftps = FTP_TLS(server)
    ftps.login(user=username, passwd=password)
    ftps.prot_p()  # Secure data connection
    return ftps


def ensure_directory_exists(ftps, path):
    """
    Ensure that the directory path exists on the FTPS server.
    """
    dirs = path.strip('/').split('/')
    current_path = ''
    for directory in dirs:
        current_path += f'/{directory}'
        try:
            ftps.cwd(current_path)
        except:
            ftps.mkd(current_path)
            ftps.cwd(current_path)


def upload_specific_file(ftps, local_file_path, remote_directory_path):
    """
    Upload a specific file to the FTPS server.
    """
    if os.path.exists(local_file_path):
        remote_file_name = os.path.basename(local_file_path)
        ensure_directory_exists(ftps, remote_directory_path)
        ftps.cwd(remote_directory_path)
        with open(local_file_path, 'rb') as file:
            ftps.storbinary(f'STOR {remote_file_name}', file)
        print(f"File '{local_file_path}' has been uploaded to '{remote_directory_path}' on the server")
    else:
        raise FileNotFoundError(f"File '{local_file_path}' not found")


def list_files_on_server(ftps, path='/'):
    """
    List files in a directory on the FTPS server.
    """
    ftps.cwd(path)
    ftps.retrlines('LIST')


def upload_csv_ctrl_to_ftp_server(default_emails: Dict[str, List[str]],
                                  slack_webhook: Optional[str] = None,
                                  **kwargs):
    """
    Upload files to FTPS server with error handling and notifications.
    """
    try:
        ti = kwargs['task_instance']
        dag_run = kwargs['dag_run']
        dag_id = ti.dag_id
        run_id = dag_run.run_id
        conf = dag_run.conf or {}

        # ดึงข้อมูล state ปัจจุบัน
        batch_state = get_batch_state(dag_id, run_id)

        # Skip task if configured to do so
        should_upload_ftp = ti.xcom_pull(key='should_upload_ftp', task_ids='validate_input')
        if not should_upload_ftp:
            print("Skipping FTPS upload as configured (ftp: false)")
            raise AirflowSkipException("FTPS upload disabled in configuration")

        # Get file names
        output_filename = ti.xcom_pull(dag_id=dag_id, key='output_filename')
        output_filename_ctrl = ti.xcom_pull(dag_id=dag_id, key='control_filename')

        if not output_filename or not output_filename_ctrl:
            raise AirflowException("Missing file names from previous tasks")

        # print("output_filename:", output_filename)
        # print("control_filename:", output_filename_ctrl)
        # print("dag_id:", dag_id)

        # Prepare paths
        csv_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/{dag_id}/'
        ctrl_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/{dag_id}/'
        csv_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename}.csv'
        ctrl_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename}.ctrl'

        # Verify local files exist
        if not os.path.exists(csv_local_file_path):
            raise FileNotFoundError(f"CSV file not found: {csv_local_file_path}")
        if not os.path.exists(ctrl_local_file_path):
            raise FileNotFoundError(f"Control file not found: {ctrl_local_file_path}")

        try:
            # Connect to FTPS
            ftps_server = '192.168.1.111'
            username = 'airflow'
            password = 'airflow'
            ftps = connect_to_ftps(ftps_server, username, password)

            try:
                # Upload files
                print("Uploading CSV file...")
                upload_specific_file(ftps, csv_local_file_path, csv_remote_path)

                print("Uploading Control file...")
                upload_specific_file(ftps, ctrl_local_file_path, ctrl_remote_path)

                print("Listing files on server...")
                list_files_on_server(ftps)

            except (error_perm, error_temp) as e:
                raise AirflowException(f"FTPS upload error: {str(e)}")
            finally:
                try:
                    ftps.quit()
                except:
                    pass  # Ignore errors during quit

        except Exception as e:
            raise AirflowException(f"FTPS connection error: {str(e)}")

    except Exception as e:
        error_msg = str(e)
        ti.xcom_push(key='error_message', value=error_msg)

        # บันทึกสถานะ FAILED ลงฐานข้อมูล
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            current_page=batch_state.get('current_page', 1) if batch_state else 1,
            last_search_after=batch_state.get('last_search_after') if batch_state else None,
            status='FAILED',
            error_message=f"FTP upload failed: {error_msg}",
            total_records=batch_state.get('total_records') if batch_state else None,
            fetched_records=batch_state.get('fetched_records', 0) if batch_state else 0
        )

        # Don't send retry notification for skipped tasks
        if not isinstance(e, AirflowSkipException):
            # Get retry information
            try_number = ti.try_number
            max_retries = ti.max_tries

            # Send retry notification if this is not the last retry
            if try_number <= max_retries:
                send_retry_notification(
                    dag_id=dag_id,
                    run_id=run_id,
                    error_message=error_msg,
                    retry_count=try_number,
                    max_retries=max_retries,
                    conf=conf,
                    default_emails=default_emails,
                    slack_webhook=slack_webhook,
                    context=kwargs
                )

        raise
