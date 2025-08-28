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
    schedule_interval="0 6-22/3 * * *",
    catchup=False,
)

# Definieer de taken van de DAG
leden_taak = BashOperator(
        task_id='leden_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables Leden"),
        dag=dag,
)

lessen_taak = BashOperator(
        task_id='lessen_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables Lessen"),
        dag=dag,
)

lesdeelname_taak = BashOperator(
        task_id='lesdeelname_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables Lesdeelname"),
        dag=dag,
)

grootboekrekening_taak = BashOperator(
        task_id='grootboekrekening_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables Grootboekrekening"),
        dag=dag,
)

omzet_taak = BashOperator(
        task_id='omzet_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables Omzet"),
        dag=dag,
)

product_verkoop_taak = BashOperator(
        task_id='product_verkoop_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables ProductVerkopen"),
        dag=dag,
)

uitbetalingen_taak = BashOperator(
        task_id='uitbetalingen_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables Uitbetalingen"),
        dag=dag,
)

openstaande_facturen_taak = BashOperator(
        task_id='openstaande_facturen_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables OpenstaandeFacturen"),
        dag=dag,
)

abonnement_statistieken_taak = BashOperator(
        task_id='abonnement_statistieken_taak',
        bash_command=venv_command("/home/greit/klanten/tree11/pipeline/main.py --tables AbonnementStatistiekenSpecifiek"),
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
    leden_taak, lessen_taak, lesdeelname_taak, grootboekrekening_taak, omzet_taak, product_verkoop_taak, uitbetalingen_taak, openstaande_facturen_taak, abonnement_statistieken_taak
] >> end_parallel_tasks
                          