from components.database import get_batch_state, get_failed_batch_runs
from airflow.exceptions import AirflowException, AirflowSkipException
from components.constants import THAI_TZ
import time
from datetime import datetime
from components.utils import get_thai_time

def check_previous_failed_batch(**context):
    """Check and resume specified or all failed batches before running current batch"""
    from airflow.models import DagRun, TaskInstance, DagBag, XCom
    from airflow.utils.state import State
    from airflow.utils.session import create_session
    import pendulum
    import time
    
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    conf = dag_run.conf or {}
    current_run_id = dag_run.run_id

    check_fail = conf.get('check_fail', True)  # Default to True if not specified
    if not check_fail:
        print("Skipping check_previous_fails as configured (check_fail: false)")
        raise AirflowSkipException("Skipping failed batch check as configured")

    # Wait for start_run if specified
    start_run = conf.get('start_run')
    if start_run:
        start_time = datetime.strptime(start_run, '%Y-%m-%d %H:%M:%S.%f')
        start_time = THAI_TZ.localize(start_time)
        current_time = get_thai_time()
        
        if current_time < start_time:
            wait_time = (start_time - current_time).total_seconds()
            print(f"Waiting {wait_time} seconds until start time: {start_run}")
            time.sleep(wait_time)
    
    # Get failed batches based on config or all failed runs
    print("\nChecking for failed batches...")
    run_ids_to_process = conf.get('run_id', [])
    
    if run_ids_to_process:
        # If specific run_ids provided, get their batch information
        print(f"Processing specified run_ids: {run_ids_to_process}")
        failed_batches = [
            batch for batch in get_failed_batch_runs(dag_id)
            if batch['run_id'] in run_ids_to_process
        ]
        # Sort by DAG start date to maintain chronological order
        failed_batches.sort(key=lambda x: x['dag_start_date'])
    else:
        # Get all failed batches ordered by start date
        print("No specific run_ids provided, processing all failed batches")
        failed_batches = get_failed_batch_runs(dag_id)
    
    if failed_batches:
        print(f"\nFound {len(failed_batches)} failed batches to process")
        print("Processing in chronological order (oldest first):")
        for i, batch in enumerate(failed_batches, 1):
            run_id = batch['run_id']
            execution_date = batch['execution_date']
            print(f"\nProcessing batch {i} of {len(failed_batches)}")
            print(f"Run ID: {run_id}")
            print(f"Execution Date: {execution_date}")
            
            try:
                with create_session() as session:
                    old_dag_run = session.query(DagRun).filter(
                        DagRun.dag_id == dag_id,
                        DagRun.run_id == run_id
                    ).first()
                    
                    if old_dag_run:
                        print(f"\nResetting tasks for DAG run: {run_id}")
                        
                        # Reset all task instances
                        task_instances = session.query(TaskInstance).filter(
                            TaskInstance.dag_id == dag_id,
                            TaskInstance.run_id == run_id
                        ).all()
                        
                        for ti in task_instances:
                            if ti.task_id == 'check_previous_failed_batch':
                                ti.state = State.SUCCESS
                            elif ti.task_id == 'validate_input':
                                ti.state = State.NONE
                            else:
                                ti.state = State.NONE
                            
                            # Clear XCom data except for check_previous_failed_batch
                            if ti.task_id != 'check_previous_failed_batch':
                                session.query(XCom).filter(
                                    XCom.dag_id == dag_id,
                                    XCom.task_id == ti.task_id,
                                    XCom.run_id == run_id
                                ).delete()
                        
                        # Reset DAG run state
                        old_dag_run.state = State.QUEUED
                        session.commit()
                        print(f"Reset complete for DAG run: {run_id}")
                        
                        # Wait for this batch to complete before proceeding to next
                        max_wait_time = 3600  # 1 hour
                        start_wait_time = time.time()
                        
                        print(f"\nWaiting for batch {run_id} to complete...")
                        while True:
                            if time.time() - start_wait_time > max_wait_time:
                                raise AirflowException(
                                    f"Timeout waiting for batch {run_id} to complete"
                                )
                            
                            session.refresh(old_dag_run)
                            current_state = old_dag_run.state
                            batch_state = get_batch_state(dag_id, run_id)
                            
                            print(f"Current state - DAG: {current_state}, "
                                  f"Batch: {batch_state['status'] if batch_state else 'Unknown'}")
                            
                            if current_state in ['success', 'failed']:
                                if current_state == 'failed' or (batch_state and batch_state['status'] == 'FAILED'):
                                    error_msg = batch_state.get('error_message') if batch_state else "Unknown error"
                                    raise AirflowException(
                                        f"Failed to resume batch {run_id}. Error: {error_msg}"
                                    )
                                print(f"Batch {run_id} completed successfully")
                                break
                            
                            time.sleep(10)
                    else:
                        print(f"Warning: Could not find DAG run for run_id: {run_id}")
                
            except Exception as e:
                print(f"Error processing batch {run_id}: {str(e)}")
                raise AirflowException(
                    f"Failed while processing batch {i} of {len(failed_batches)}. "
                    f"Run ID: {run_id}, Error: {str(e)}"
                )
        
        print("\nAll failed batches have been processed successfully")
    else:
        print("No failed batches found, proceeding with current batch")