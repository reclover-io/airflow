from components.database import get_batch_state, get_failed_batch_runs
from airflow.exceptions import AirflowException
from components.constants import THAI_TZ
import time
from datetime import datetime
from components.utils import get_thai_time

def check_previous_failed_batch(**context):
    """Check and resume specified or all failed batches sequentially"""
    from airflow.models import DagRun, TaskInstance, DagBag, XCom
    from airflow.utils.state import State
    from airflow.utils.session import create_session
    import pendulum
    import time
    
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    conf = dag_run.conf or {}
    current_run_id = dag_run.run_id

    # Wait for start_run if specified
    start_run = conf.get('start_run')
    if start_run:
        start_time = datetime.strptime(start_run, '%Y-%m-%d %H:%M:%S.%f')
        start_time = THAI_TZ.localize(start_time)
        current_time = get_thai_time()
        
        if current_time < start_time:
            wait_seconds = (start_time - current_time).total_seconds()
            print(f"\nWaiting for scheduled start time: {wait_seconds:.0f} seconds")
            time.sleep(wait_seconds)
    
    # Get failed batches based on config or all failed runs
    print("\nChecking for failed batches...")
    run_ids_to_process = conf.get('run_id', [])
    
    if run_ids_to_process:
        failed_batches = [
            batch for batch in get_failed_batch_runs(dag_id)
            if batch['run_id'] in run_ids_to_process
        ]
        failed_batches.sort(key=lambda x: x['dag_start_date'])
    else:
        failed_batches = get_failed_batch_runs(dag_id)

    if failed_batches:
        print(f"\nFound {len(failed_batches)} failed batches to process")
        print("Processing in chronological order (oldest first):")

        # Process each failed batch sequentially
        for i, batch in enumerate(failed_batches, 1):
            run_id = batch['run_id']
            
            try:
                print(f"\nProcessing batch {i} of {len(failed_batches)}")
                print(f"Run ID: {run_id}")

                # Wait until there are no active runs
                while True:
                    with create_session() as session:
                        active_runs = session.query(DagRun).filter(
                            DagRun.dag_id == dag_id,
                            DagRun.state.in_(['running', 'queued']),
                            DagRun.run_id != current_run_id,  # Exclude current run
                            DagRun.run_id != run_id  # Exclude the run we're about to process
                        ).count()
                        
                        if active_runs == 0:
                            break
                        
                        print(f"Waiting for {active_runs} active runs to complete...")
                        time.sleep(30)  # Check every 30 seconds

                with create_session() as session:
                    old_dag_run = session.query(DagRun).filter(
                        DagRun.dag_id == dag_id,
                        DagRun.run_id == run_id
                    ).first()
                    
                    if old_dag_run:
                        print(f"Resetting tasks for DAG run: {run_id}")
                        
                        # Reset tasks
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
                            
                            if ti.task_id != 'check_previous_failed_batch':
                                session.query(XCom).filter(
                                    XCom.dag_id == dag_id,
                                    XCom.task_id == ti.task_id,
                                    XCom.run_id == run_id
                                ).delete()

                        old_dag_run.state = State.QUEUED
                        session.commit()

                        # Wait for this batch to complete
                        max_wait_time = 3600  # 1 hour
                        start_wait_time = time.time()
                        
                        while True:
                            if time.time() - start_wait_time > max_wait_time:
                                raise AirflowException(f"Timeout waiting for batch {run_id}")
                            
                            session.refresh(old_dag_run)
                            current_state = old_dag_run.state
                            batch_state = get_batch_state(dag_id, run_id)
                            
                            if current_state in ['success', 'failed']:
                                if current_state == 'failed':
                                    error_msg = batch_state.get('error_message') if batch_state else "Unknown error"
                                    raise AirflowException(f"Failed to resume batch {run_id}: {error_msg}")
                                break
                            
                            time.sleep(10)
                            
                        print(f"Batch {run_id} completed successfully")
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