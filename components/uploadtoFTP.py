import os
import subprocess
from airflow.exceptions import AirflowSkipException, AirflowException
from typing import Optional, Dict, List
from components.notifications import send_retry_notification
from components.database import (
    get_batch_state, 
    save_batch_state,
)
import time

host_ftps = "34.124.138.144"
username_ftps = "airflow"
password_ftps = "airflow"

def run_lftp(host, username, password, local_file, remote_path, local_file_ctrl, remote_path_ctrl, ti=None):
    """
    Connect to an FTPS server using lftp and upload files.
    """
    #  mkdir -p /10.250.1.101/ELK/daily/source_data/landing/ELK_MobileApp_Activity_Logs/
    lftp_command = f"""
    lftp -u {username},{password} {host} <<EOF
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
            text=True,
            timeout=300
        )

        if result.returncode != 0:
            error_msg = f"Upload file to FTP Server failed because failed to connect to FTP Server."
            ti.xcom_push(key='error_message', value=error_msg)
            raise AirflowException(error_msg)

        print(f"Files uploaded successfully to {remote_path} and {remote_path_ctrl}.")

    except subprocess.TimeoutExpired:
        error_msg = "Upload file to FTP Server failed because failed to connect to FTP Server."
        ti.xcom_push(key='error_message', value=error_msg)
        raise AirflowException(error_msg)

    except Exception as e:
        error_msg = f"Upload file to FTP Server failed because failed to connect to FTP Server."
        ti.xcom_push(key='error_message', value=error_msg)
        raise AirflowException(f"Upload file to FTP Server failed because failed to connect to FTP Server.")


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
        run_id_conf = conf.get('run_id', None)
        if run_id_conf:
            raise AirflowSkipException(f"Skipping notification because get run_id: {run_id_conf}")

        batch_state = get_batch_state(dag_id, run_id)

        # Check if upload should be skipped
        should_upload_ftp = ti.xcom_pull(key='should_upload_ftp', task_ids='validate_input')
        if not should_upload_ftp:
            raise AirflowSkipException("FTPS upload disabled in configuration")

        # Get file names from XCom
        output_filename_csv = ti.xcom_pull(dag_id=dag_id, key='output_filename')
        output_filename_ctrl = ti.xcom_pull(dag_id=dag_id, key='control_filename')

        # Validate file names
        if not output_filename_csv or not output_filename_ctrl:
            raise AirflowException("Missing file names from previous tasks")

        # Prepare file paths
        csv_remote_path = f'/ELK/daily/source_data/landing/{dag_id}/{output_filename_csv}'
        ctrl_remote_path = f'/ELK/daily/source_data/landing/{dag_id}/{output_filename_ctrl}'

        remote_path = f'/ELK/daily/source_data/landing/{dag_id}/'

        ti.xcom_push(key='remote_path', value=remote_path)

        csv_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename_csv}'
        ctrl_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename_ctrl}'

        #time.sleep(20)
        # Verify local files exist
        if not os.path.exists(csv_local_file_path) or not os.path.exists(ctrl_local_file_path):
            print(f"CSV file not found: {csv_local_file_path} and Control file: {ctrl_local_file_path}")
            error_msg = "Upload file to FTP Server failed because can't find the file to upload."
            ti.xcom_push(key='error_message', value=error_msg)
            save_batch_state(
                batch_id=dag_id,
                run_id=run_id,
                start_date=conf.get('startDate'),
                end_date=conf.get('endDate'),
                csv_filename=output_filename_csv,
                ctrl_filename=output_filename_ctrl,
                current_page=batch_state.get('current_page', 1) if batch_state else 1,
                last_search_after=batch_state.get('last_search_after') if batch_state else None,
                status='FAILED',
                error_message=f"FTP upload failed: {error_msg}",
                total_records=batch_state.get('total_records') if batch_state else None,
                fetched_records=batch_state.get('fetched_records', 0) if batch_state else 0
            )
            raise AirflowException(error_msg)

        # Run lftp to upload files
        print(f"Starting upload of {csv_local_file_path} and {ctrl_local_file_path}...")
        run_lftp(
            host=host_ftps,
            username=username_ftps,
            password=password_ftps,
            local_file=csv_local_file_path,
            remote_path=csv_remote_path,
            local_file_ctrl=ctrl_local_file_path,
            remote_path_ctrl=ctrl_remote_path,
            ti=ti
        )

        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            csv_filename=output_filename_csv,
            ctrl_filename=output_filename_ctrl,
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
        error_msg = ti.xcom_pull(key='error_message')
        print(f"FTPS connection error: {str(e)}")
        print(error_msg)

        # Push error to XCom for downstream tasks
        ti.xcom_push(key='error_message', value=error_msg)

        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            csv_filename=output_filename_csv,
            ctrl_filename=output_filename_ctrl,
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

        raise AirflowException(error_msg)
    
def upload_csv_ctrl_to_ftp_server_monthly(default_emails: Dict[str, List[str]],
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
        run_id_conf = conf.get('run_id', None)
        if run_id_conf:
            raise AirflowSkipException(f"Skipping notification because get run_id: {run_id_conf}")

        batch_state = get_batch_state(dag_id, run_id)

        # Check if upload should be skipped
        should_upload_ftp = ti.xcom_pull(key='should_upload_ftp', task_ids='validate_input_task_monthly')
        if not should_upload_ftp:
            raise AirflowSkipException("FTPS upload disabled in configuration")

        # Get file names from XCom
        output_filename_csv = ti.xcom_pull(dag_id=dag_id, key='output_filename')
        output_filename_ctrl = ti.xcom_pull(dag_id=dag_id, key='control_filename')

        # Validate file names
        if not output_filename_csv or not output_filename_ctrl:
            raise AirflowException("Missing file names from previous tasks")

        # Prepare file paths
        csv_remote_path = f'/ELK/monthly/source_data/landing/{dag_id}/{output_filename_csv}'
        ctrl_remote_path = f'/ELK/monthly/source_data/landing/{dag_id}/{output_filename_ctrl}'

        remote_path = f'/ELK/monthly/source_data/landing/{dag_id}/'

        ti.xcom_push(key='remote_path', value=remote_path)

        csv_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename_csv}'
        ctrl_local_file_path = f'/opt/airflow/data/batch/{dag_id}/{output_filename_ctrl}'

        #time.sleep(20)
        # Verify local files exist
        if not os.path.exists(csv_local_file_path) or not os.path.exists(ctrl_local_file_path):
            print(f"CSV file not found: {csv_local_file_path} and Control file: {ctrl_local_file_path}")
            error_msg = "Upload file to FTP Server failed because can't find the file to upload."
            ti.xcom_push(key='error_message', value=error_msg)
            save_batch_state(
                batch_id=dag_id,
                run_id=run_id,
                start_date=conf.get('startDate'),
                end_date=conf.get('endDate'),
                csv_filename=output_filename_csv,
                ctrl_filename=output_filename_ctrl,
                current_page=batch_state.get('current_page', 1) if batch_state else 1,
                last_search_after=batch_state.get('last_search_after') if batch_state else None,
                status='FAILED',
                error_message=f"FTP upload failed: {error_msg}",
                total_records=batch_state.get('total_records') if batch_state else None,
                fetched_records=batch_state.get('fetched_records', 0) if batch_state else 0
            )
            raise AirflowException(error_msg)

        # Run lftp to upload files
        print(f"Starting upload of {csv_local_file_path} and {ctrl_local_file_path}...")
        run_lftp(
            host=host_ftps,
            username=username_ftps,
            password=password_ftps,
            local_file=csv_local_file_path,
            remote_path=csv_remote_path,
            local_file_ctrl=ctrl_local_file_path,
            remote_path_ctrl=ctrl_remote_path,
            ti=ti
        )

        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            csv_filename=output_filename_csv,
            ctrl_filename=output_filename_ctrl,
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
        error_msg = ti.xcom_pull(key='error_message')
        print(f"FTPS connection error: {str(e)}")
        print(error_msg)

        # Push error to XCom for downstream tasks
        ti.xcom_push(key='error_message', value=error_msg)

        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            csv_filename=output_filename_csv,
            ctrl_filename=output_filename_ctrl,
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

        raise AirflowException(error_msg)

def upload_csv_ctrl_to_ftp_server_manual(default_emails: Dict[str, List[str]],
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
        ftp_path = conf.get('FTP_PATH','')
        batch_state = get_batch_state(dag_id, run_id)

        # Check if upload should be skipped
        should_upload_ftp = ti.xcom_pull(key='should_upload_ftp', task_ids='validate_input_task_manual')
        if not should_upload_ftp:
            raise AirflowSkipException("FTPS upload disabled in configuration")

        # Get file names from XCom
        output_filename_csv = ti.xcom_pull(dag_id=dag_id, key='output_filename')
        output_filename_ctrl = ti.xcom_pull(dag_id=dag_id, key='control_filename')
        dag_name = ti.xcom_pull(dag_id=dag_id, key='dag_name')
        # Validate file names
        if not output_filename_csv or not output_filename_ctrl:
            raise AirflowException("Missing file names from previous tasks")

        # Prepare file paths
        csv_remote_path = f'{ftp_path}{output_filename_csv}'
        ctrl_remote_path = f'{ftp_path}{output_filename_ctrl}'

        remote_path = f'{ftp_path}'
        ti.xcom_push(key='remote_path', value=remote_path)

        csv_local_file_path = f'/opt/airflow/data/batch/{dag_name}/{output_filename_csv}'
        ctrl_local_file_path = f'/opt/airflow/data/batch/{dag_name}/{output_filename_ctrl}'

        # Verify local files exist
        if not os.path.exists(csv_local_file_path) or not os.path.exists(ctrl_local_file_path):
            print(f"CSV file not found: {csv_local_file_path} and Control file: {ctrl_local_file_path}")
            error_msg = "Upload file to FTP Server failed because can't find the file to upload."
            ti.xcom_push(key='error_message', value=error_msg)
            save_batch_state(
                batch_id=dag_id,
                run_id=run_id,
                start_date=conf.get('startDate'),
                end_date=conf.get('endDate'),
                csv_filename=output_filename_csv,
                ctrl_filename=output_filename_ctrl,
                current_page=batch_state.get('current_page', 1) if batch_state else 1,
                last_search_after=batch_state.get('last_search_after') if batch_state else None,
                status='FAILED',
                error_message=f"FTP upload failed: {error_msg}",
                total_records=batch_state.get('total_records') if batch_state else None,
                fetched_records=batch_state.get('fetched_records', 0) if batch_state else 0
            )
            raise AirflowException(error_msg)

        # Run lftp to upload files
        print(f"Starting upload of {csv_local_file_path} and {ctrl_local_file_path}...")
        run_lftp(
            host=host_ftps,
            username=username_ftps,
            password=password_ftps,
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
            csv_filename=output_filename_csv,
            ctrl_filename=output_filename_ctrl,
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
            csv_filename=output_filename_csv,
            ctrl_filename=output_filename_ctrl,
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

