from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.db import provide_session
from datetime import datetime, timedelta
import requests
import json
import pytz

@provide_session
def create_spark_connection(session=None):
    conn = Connection(
        conn_id='spark_default',
        conn_type='spark',
        host='spark://spark', 
        port=7077,
        extra='{"deploy-mode": "client"}' 
    )
    
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()
        print("Koneksi Spark berhasil dibuat")
    else:
        print("Koneksi Spark sudah ada")

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


create_spark_connection()
default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': discord_alert,
    'provide_context': True
}
with DAG(
    dag_id='spark_elt',
    start_date=datetime(2025, 1, 2),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=['spark', 'minio', 'discordAlerts']
) as dag:

    start = EmptyOperator(task_id='start')
    
    conn_check = PythonOperator(
        task_id='check_spark_connection',
        python_callable=create_spark_connection
    )

    elt_job = SparkSubmitOperator(
        task_id="elt_job",
        conn_id="spark_default",
        name="extract_load",
        application="/opt/airflow/apps/main.py",
        verbose=True,
         application_args=["--load_date={{ ds }}",
                           "--data_date={{ prev_ds }}"
                          ],
        properties_file = "/opt/airflow/dags/spark-defaults.conf",
        jars = ','.join([
            '/opt/airflow/jars/postgresql-42.7.4.jar',
            '/opt/airflow/jars/hadoop-aws-3.3.4.jar',
            '/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar',
            '/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar',
            '/opt/airflow/jars/iceberg-aws-bundle-1.8.1.jar'
        ])
    )

    end = EmptyOperator(task_id='end')

    start >> conn_check >> elt_job >> end