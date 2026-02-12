from airflow.decorators import dag
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def on_failure_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    error = context.get('exception')
    
    print(f"Task {task_id} in DAG {dag_id} failed")
    print(f"Error Details: {error}")


@dag(
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    on_failure_callback=on_failure_callback,
    catchup=False,
    tags=['ingestor']
)
def ingestor_orchestrator():

    wait_for_file= FileSensor(
        task_id='wait_for_file',
        filepath= '*',
        fs_conn_id='fs_bronze',
        poke_interval=5,
        timeout=10,
        retries=2,                    
        retry_delay=timedelta(minutes=5),
        mode='poke',
        soft_fail=True,
    )

    run_python_script = BashOperator(
    task_id='run_my_python_script',
    bash_command='python /opt/workspace/ingest_to_snowflake.py',
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_analytics_transformation',
        wait_for_completion=False,
        reset_dag_run=True
    )

    wait_for_file >> run_python_script >> trigger_dbt

ingestor_orchestrator()