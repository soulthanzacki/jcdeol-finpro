from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.generator import generate_patients, generate_visits, generate_details
from scripts.postgres import table_check, data_check, insertData_patients, insertData_visits, insertData_details
from datetime import timedelta
import requests
import json
import pytz

def discord_alert(context):
    webhook_url = "https://discord.com/api/webhooks/1364231145792999455/h33HP4WmoKKT-x25UXQIysy0ZGlIis2FN-L9pfuQe8CPD50vQo_nN9NRcLHE3cnhQaYg"
    wib_tz = pytz.timezone("Asia/Jakarta")

    task_instance = context['task_instance']
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date'].astimezone(wib_tz)
    
    message = f"DAG Failed: {dag_id}\nTask: {task_id}\nExecution Date: {execution_date.strftime("%Y-%m-%d %H:%M:%S")}"
    
    data = {"content": message}
    response = requests.post(webhook_url, data=json.dumps(data), headers={"Content-Type": "application/json"})
    
    if response.status_code != 204:
        print(f"Request to Discord returned an error {response.status_code}, response: {response.text}")

def postgres_prep():
    table_check()
    
def generate_patient(**kwargs):
    patient_fetch = data_check('patients')
    new_patients, upd_patients = generate_patients(patient_fetch)
    kwargs['ti'].xcom_push(key='patient_data_new', value=new_patients)
    kwargs['ti'].xcom_push(key='patient_data_upd', value=upd_patients)

def insert_patients(**kwargs):
    new_patients = kwargs['ti'].xcom_pull(task_ids='Patients.generate_patients_data', key='patient_data_new')
    update_patients = kwargs['ti'].xcom_pull(task_ids='Patients.generate_patients_data', key='patient_data_upd')
    insertData_patients(new_patients, update_patients)

def generate_visit(**kwargs):
    result, patient_fetch = data_check('visits')
    id_list = len(result)
    visits = generate_visits(id_list, patient_fetch)
    kwargs['ti'].xcom_push(key='visit_data', value=visits)

def insert_visits(**kwargs):
    visits = kwargs['ti'].xcom_pull(task_ids='Visits.generate_visits_data', key='visit_data')
    insertData_visits(visits)

def generate_detail(**kwargs):
    result = data_check('details')
    id_list = len(result)
    details = generate_details(id_list)
    kwargs['ti'].xcom_push(key='detail_data', value=details)

def insert_details(**kwargs):
    details = kwargs['ti'].xcom_pull(task_ids='Details.generate_details_data', key='detail_data')
    insertData_details(details)

default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': discord_alert,
    'provide_context': True
}
with DAG(
    dag_id='data_generator',
    start_date=datetime(2025,4,19),
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['generate', 'insert', 'discordAlerts'],
    default_args=default_args
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    prep = PythonOperator(task_id='prep', python_callable=postgres_prep)

    with TaskGroup("Patients") as patients_data:
        generate_data = PythonOperator(task_id='generate_patients_data', python_callable=generate_patient)
        insert_data = PythonOperator(task_id='insert_patients_data', python_callable=insert_patients)

        generate_data >> insert_data

    with TaskGroup("Visits") as visits_data:
        generate_data = PythonOperator(task_id='generate_visits_data', python_callable=generate_visit)
        insert_data = PythonOperator(task_id='insert_visits_data', python_callable=insert_visits)

        generate_data >> insert_data

    with TaskGroup("Details") as details_data:
        generate_data = PythonOperator(task_id='generate_details_data', python_callable=generate_detail)
        insert_data = PythonOperator(task_id='insert_details_data', python_callable=insert_details)

        generate_data >> insert_data

    end = EmptyOperator(task_id='end')

    start >> prep >> patients_data >> [visits_data, details_data] >> end