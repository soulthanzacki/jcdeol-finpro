from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.generator import generate_patients, generate_visits, generate_details
from scripts.postgres import table_check, data_check, insertData_patients, insertData_visits, insertData_details

def postgres_prep():
    table_check()
    
def generate_patient(**kwargs):
    id_list = len(data_check('patients'))
    patients = generate_patients(id_list)
    kwargs['ti'].xcom_push(key='patient_data', value=patients)

def insert_patients(**kwargs):
    patients = kwargs['ti'].xcom_pull(task_ids='Patients.generate_patients_data', key='patient_data')
    insertData_patients(patients)

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

with DAG(
    dag_id='data_generator',
    start_date=datetime(2025, 4, 9),
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['generate', 'insert']
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