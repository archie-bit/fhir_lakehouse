from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime



@dag(
    schedule=None, # This is important! It only runs when triggered
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt_analytics_transformation']
)
def dbt_analytics_transformation():
    
    run_dbt = BashOperator(
        task_id='dbt_run',
        bash_command="cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt deps && /home/airflow/.local/bin/dbt run --profiles-dir /home/airflow/.dbt",
        env={'DBT_PROFILES_DIR': '/home/airflow/.dbt'}
    )
    
    test_dbt = BashOperator(
        task_id='dbt_test',
        bash_command="cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --profiles-dir /home/airflow/.dbt",
        env={'DBT_PROFILES_DIR': '/home/airflow/.dbt'}
    )

    run_dbt >> test_dbt

dbt_analytics_transformation()