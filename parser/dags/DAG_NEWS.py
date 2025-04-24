from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'run_tg_news_script_dag',
    default_args=default_args,
    description='Запуск TG_NEWS.py каждые 5 минут',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(minutes=5),  # Интервал выполнения
    catchup=False,
    max_active_runs=1  # Ограничение на количество одновременно выполняющихся DAGов
) as dag:

    task_tg_news = BashOperator(
        task_id='run_tg_news_script',
        bash_command="python /opt/airflow/dags/scripts/TG_NEWS.py",
    )