from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Venv activerings functie
def venv_command(script_path):
    return f"source /home/greit/klanten/tree11/tree11_venv/bin/activate && python3 {script_path}"


# Definieer de standaardinstellingen voor de DAG
default_args = {
    'owner': 'Max - Greit',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['max@greit.nl'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definieer de DAG
dag = DAG(
    'tree11_daily_dag_v01',
    default_args=default_args,
    description='Data update',
    schedule_interval="0 7 * * *",
    catchup=False,
)

# Definieer de taken van de DAG
dagelijkse_taak = BashOperator(
        task_id='dagelijkse_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py"),
        dag=dag,
)

start_parallel_tasks = EmptyOperator(
        task_id='start_parallel_tasks',
        dag=dag,
    )

end_parallel_tasks = EmptyOperator(
        task_id='end_parallel_tasks',
        dag=dag,
    )

# Taak structuur
start_parallel_tasks >> [
    dagelijkse_taak,
] >> end_parallel_tasks
                          