import os
import subprocess
from airflow.exceptions import AirflowSkipException, AirflowException
from typing import Optional, Dict, List
from components.notifications import send_retry_notification
from components.database import (
    get_batch_state, 
    save_batch_state,
)


def run_lftp(host, username, password, local_file, remote_path, local_file_ctrl, remote_path_ctrl):
    """
    Connect to an FTPS server using lftp and upload files.
    """
    #  mkdir -p /10.250.1.101/ELK/daily/source_data/landing/ELK_MobileApp_Activity_Logs/
    lftp_command = f"""
    lftp -u {username},{password} ftp://{host} <<EOF
    set ssl:verify-certificate no
   
    put {local_file} -o {remote_path}
    put {local_file_ctrl} -o {remote_path_ctrl}
    bye
    EOF
    """
    try:
        result = subprocess.run(
            lftp_command,
            shell=True,
            capture_output=True,
            text=True
        )
        

        # Check return code
        if result.returncode != 0:
            raise AirflowException(f"lftp failed with return code {result.returncode}: {result.stderr}")

        print(f"Files uploaded successfully to {remote_path} and {remote_path_ctrl}.")

    except Exception as e:
        raise AirflowException(f"An error occurred while running lftp: {e}")


def upload_csv_ctrl_to_ftp_server(default_emails: Dict[str, List[str]],
                                  slack_webhook: Optional[str] = None,
                                  **kwargs):
    """
    Upload files to FTPS server using lftp with error handling and notifications.
    """
    try:
        # Extract context from Airflow
        ti = kwargs['task_instance']
        dag_run = kwargs['dag_run']
        dag_id = ti.dag_id
        run_id = dag_run.run_id
        conf = dag_run.conf or {}

        batch_state = get_batch_state(dag_id, run_id)

        # Check if upload should be skipped
        should_upload_ftp = ti.xcom_pull(key='should_upload_ftp', task_ids='validate_input')
        if not should_upload_ftp:
            raise AirflowSkipException("FTPS upload disabled in configuration")

        # Get file names from XCom
        output_filename_csv = ti.xcom_pull(dag_id=dag_id, key='output_filename')
        output_filename_ctrl = ti.xcom_pull(dag_id=dag_id, key='control_filename')
        dag_path = ti.xcom_pull(dag_id=dag_id,key='dag_path', task_ids='process_data')


        # Validate file names
        if not output_filename_csv or not output_filename_ctrl:
            raise AirflowException("Missing file names from previous tasks")

        # Prepare file paths
        csv_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/{dag_path}/{output_filename_csv}'
        ctrl_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/{dag_path}/{output_filename_ctrl}'

        csv_local_file_path = f'/opt/airflow/data/batch/{dag_path}/{output_filename_csv}'
        ctrl_local_file_path = f'/opt/airflow/data/batch/{dag_path}/{output_filename_ctrl}'

        # Verify local files exist
        if not os.path.exists(csv_local_file_path):
            print(f"CSV file not found: {csv_local_file_path}")
            raise FileNotFoundError(f"Upload file to FTP Server failed because can't find the file to upload.")
        if not os.path.exists(ctrl_local_file_path):
            print(f"Control file not found: {ctrl_local_file_path}")
            raise FileNotFoundError(f"Upload file to FTP Server failed because can't find the file to upload.")

        # Run lftp to upload files
        print(f"Starting upload of {csv_local_file_path} and {ctrl_local_file_path}...")
        run_lftp(
            host='34.124.138.144',
            username='airflow',
            password='airflow',
            local_file=csv_local_file_path,
            remote_path=csv_remote_path,
            local_file_ctrl=ctrl_local_file_path,
            remote_path_ctrl=ctrl_remote_path
        )

        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            current_page=batch_state.get('current_page', 1) if batch_state else 1,
            last_search_after=batch_state.get('last_search_after') if batch_state else None,
            status='COMPLETED',
            error_message=None,
            total_records=batch_state.get('total_records') if batch_state else None,
            fetched_records=batch_state.get('fetched_records', 0) if batch_state else 0
        )

    except AirflowSkipException as e:
        # Handle skipped task explicitly
        print(f"Task skipped: {e}")
        raise

    except Exception as e:
        # General error handling
        error_msg = f"Upload file to FTP Server failed because failed to connect to FTP Server."
        print(f"FTPS connection error: {str(e)}")
        print(error_msg)

        # Push error to XCom for downstream tasks
        ti.xcom_push(key='error_message', value=error_msg)

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

        # Retry notification logic
        try_number = ti.try_number
        max_retries = ti.max_tries
        if try_number < max_retries:
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

        raise AirflowException(error_msg)
