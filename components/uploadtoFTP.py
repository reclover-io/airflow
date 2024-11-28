import os
import subprocess
from airflow.exceptions import AirflowSkipException, AirflowException
from typing import Optional, Dict, List
from components.notifications import send_retry_notification


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

        # Check if upload should be skipped
        should_upload_ftp = ti.xcom_pull(key='should_upload_ftp', task_ids='validate_input')
        if not should_upload_ftp:
            raise AirflowSkipException("FTPS upload disabled in configuration")

        # Get file names from XCom
        output_filename = ti.xcom_pull(dag_id=dag_id, key='output_filename')
        output_filename_ctrl = ti.xcom_pull(dag_id=dag_id, key='control_filename')

        # Validate file names
        if not output_filename or not output_filename_ctrl:
            raise AirflowException("Missing file names from previous tasks")

        # Prepare file paths
        csv_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/{dag_id}/{output_filename}.csv'
        ctrl_remote_path = f'/10.250.1.101/ELK/daily/source_data/landing/{dag_id}/{output_filename}.ctrl'

        csv_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename}.csv'
        ctrl_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename}.ctrl'

        # Verify local files exist
        if not os.path.exists(csv_local_file_path):
            raise FileNotFoundError(f"CSV file not found: {csv_local_file_path}")
        if not os.path.exists(ctrl_local_file_path):
            raise FileNotFoundError(f"Control file not found: {ctrl_local_file_path}")

        # Run lftp to upload files
        print(f"Starting upload of {csv_local_file_path} and {ctrl_local_file_path}...")
        run_lftp(
            host='192.168.0.101',
            username='airflow',
            password='airflow',
            local_file=csv_local_file_path,
            remote_path=csv_remote_path,
            local_file_ctrl=ctrl_local_file_path,
            remote_path_ctrl=ctrl_remote_path
        )

    except AirflowSkipException as e:
        # Handle skipped task explicitly
        print(f"Task skipped: {e}")
        raise

    except FileNotFoundError as e:
        # Specific case for missing files
        print(f"File error: {e}")
        raise

    except Exception as e:
        # General error handling
        error_msg = f"Error during FTP upload: {e}"
        print(error_msg)

        # Push error to XCom for downstream tasks
        ti.xcom_push(key='error_message', value=error_msg)

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
