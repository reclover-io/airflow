from components.database import get_batch_state, get_failed_batch_runs
from airflow.exceptions import AirflowException
from components.constants import THAI_TZ
import time

def check_previous_failed_batch(**context):
    """Check and resume failed batches before running current batch"""
    from airflow.models import DagRun, TaskInstance, DagBag, XCom 
    from airflow.utils.state import State
    from airflow.utils.session import create_session
    from airflow.api.common.trigger_dag import trigger_dag
    import pendulum
    
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    
    # Get failed batches
    print("\nChecking for failed batches...")
    failed_batches = get_failed_batch_runs(dag_id, None)
    
    if failed_batches:
        print(f"\nProcessing {len(failed_batches)} failed batches...")
        
        for batch in failed_batches:
            run_id = batch['run_id']
            print(f"\nResuming failed batch: {run_id}")
            print(f"Original date range: {batch['start_date']} to {batch['end_date']}")
            
            try:
                with create_session() as session:
                    # Get the original DAG run
                    old_dag_run = session.query(DagRun).filter(
                        DagRun.dag_id == dag_id,
                        DagRun.run_id == run_id
                    ).first()
                    
                    if old_dag_run:
                        # Reset ALL task instances for this DAG run
                        task_instances = session.query(TaskInstance).filter(
                            TaskInstance.dag_id == dag_id,
                            TaskInstance.run_id == run_id
                        ).all()
                        
                        print(f"Resetting all tasks for DAG run: {run_id}")
                        for ti in task_instances:
                            print(f"Resetting task: {ti.task_id}")
                            # Reset task state to None
                            ti.state = State.NONE
                            # Clear XCom data
                            session.query(XCom).filter(
                                XCom.dag_id == dag_id,
                                XCom.task_id == ti.task_id,
                                XCom.run_id == run_id
                            ).delete()
                        
                        # Reset DAG run state to QUEUED
                        old_dag_run.state = State.QUEUED
                        
                        session.commit()
                        print(f"Reset complete for DAG run: {run_id}")
                        
                        # Wait for the batch to complete
                        max_wait_time = 3600  # 1 hour
                        start_wait_time = time.time()
                        
                        while True:
                            if time.time() - start_wait_time > max_wait_time:
                                raise AirflowException(
                                    f"Timeout waiting for batch {run_id} to complete"
                                )
                            
                            # Refresh DAG run state
                            session.refresh(old_dag_run)
                            current_state = old_dag_run.state
                            
                            # Get batch state
                            batch_state = get_batch_state(dag_id, run_id)
                            print(f"Current DAG state: {current_state}, "
                                  f"Batch state: {batch_state['status'] if batch_state else 'Unknown'}")
                            
                            if current_state in ['success', 'failed']:
                                if current_state == 'failed' or (batch_state and batch_state['status'] == 'FAILED'):
                                    error_msg = batch_state.get('error_message') if batch_state else "Unknown error"
                                    raise AirflowException(
                                        f"Failed to resume batch {run_id}. Error: {error_msg}"
                                    )
                                break
                            
                            time.sleep(10)
                    else:
                        print(f"Warning: Could not find original DAG run for run_id: {run_id}")
                        
            except Exception as e:
                print(f"Error processing batch {run_id}: {str(e)}")
                raise AirflowException(f"Failed to resume batch {run_id}. Error: {str(e)}")
        
        print("\nAll failed batches have been processed")
    else:
        print("No failed batches found, proceeding with current batch")