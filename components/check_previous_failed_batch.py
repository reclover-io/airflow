from airflow.exceptions import AirflowException

def check_previous_failed_batch(**context):
    """Check and resume specified or all failed batches before running current batch"""
    from airflow.models import DagRun, TaskInstance, XCom
    from airflow.utils.state import State
    from airflow.utils.session import create_session
    from sqlalchemy import or_
    
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    conf = dag_run.conf or {}

    run_id_conf = conf.get('run_id', None)

    try:
        with create_session() as session:
            if run_id_conf:
                failed_dag_runs = session.query(DagRun).filter(
                    DagRun.state == State.FAILED,
                    DagRun.run_id.in_(run_id_conf)
                ).order_by(DagRun.execution_date.asc()).all()

            else:
                failed_dag_runs = session.query(DagRun).filter(
                    DagRun.state == State.FAILED,
                ).order_by(DagRun.execution_date.asc()).all()

            print(f"Number of failed DAG runs: {len(failed_dag_runs)}")
            for failed_dag_run in failed_dag_runs:
                print(f"Failed DAG run: {failed_dag_run.run_id}, Execution date: {failed_dag_run.execution_date}")

            for failed_dag_run in failed_dag_runs:
                print(f"Processing failed DAG run: {failed_dag_run.run_id}, Execution date: {failed_dag_run.execution_date}")
                task_instances = session.query(TaskInstance).filter(
                    TaskInstance.run_id == failed_dag_run.run_id,
                    or_(
                        (TaskInstance.task_id == 'send_running_notification'),
                        (TaskInstance.task_id == 'send_success_notification'),
                        (TaskInstance.task_id == 'send_failure_notification'),
                        (TaskInstance.task_id == 'process_data') & (TaskInstance.state == 'failed'),
                        (TaskInstance.task_id == 'check_previous_failed_batch') & (TaskInstance.state == 'failed'),
                        (TaskInstance.task_id == 'uploadtoFTP') & (TaskInstance.state == 'failed')
                    )
                ).order_by(TaskInstance.updated_at.asc()).all()

                for ti in task_instances:
                    print(f"Retrying task instance: {ti.task_id}, State: {ti.state}, Try number: {ti.try_number}")
                    ti.state = State.UP_FOR_RETRY
                    ti.try_number = 0
                    session.merge(ti)
                    # ti.state = State.NONE
                    session.query(XCom).filter(
                        XCom.dag_id == dag_id,
                        XCom.task_id == ti.task_id,
                        XCom.run_id == failed_dag_run.run_id
                    ).delete()
                
                failed_dag_run.state = State.QUEUED
                session.merge(failed_dag_run)

            session.commit()
    except Exception as e:
        context['ti'].xcom_push(key='error_message', value=str(e))
        print(f"Error: {str(e)}")
        raise AirflowException(f"Failed to check and resume failed batches: {str(e)}")