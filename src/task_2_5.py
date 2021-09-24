from airflow import DAG
from airflow.operators import DummyOperator,PythonOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),datetime.min.time())

default_args = {
    'owner': 'airflow','depends_on_past': False,'start_date': seven_days_ago,'email': ['airflow@airflow.com'],
    'email_on_failure': False,'email_on_retry': False,'retries': 1,'retry_delay': timedelta(minutes=5),
)

dag = DAG('simple', default_args=default_args)
task_1 = DummyOperator(
        task_id='testairflow',
        bash_command='/Users/juancasarescosmen/src/task_1_1.py',
        dag=dag)
task_2 = DummyOperator(
        task_id='testairflow',
        bash_command='python /Users/juancasarescosmen/src/task_1_2.py;'
                     'python /Users/juancasarescosmen/src/task_1_3.py:',
        dag=dag)
task_3 = DummyOperator(
        task_id='testairflow',
        bash_command='python /Users/juancasarescosmen/src/task_2_1.py;'
                     'python /Users/juancasarescosmen/src/task_2_2.py;',
                     'python /Users/juancasarescosmen/src/task_2_3.py;',
        dag=dag)