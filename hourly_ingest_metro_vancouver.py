import datetime
import pytz
from airflow import models
from airflow.operators import docker_operator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.utcnow(),
    'email': ['klucas.schwartz@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}


with models.DAG('hourly_ingest_metro_vancouver', schedule_interval='@hourly', default_args=default_args) as dag:

    datetime_str = (datetime.datetime.now(tz=pytz.timezone('America/Vancouver')) - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')
    docker_cmd = 'python main.py --location=vancouver --window_datetime_start={} \
    --window_length_hours=1 --cloud_function_endpoint=https://us-central1-rental-housing-project.cloudfunctions.net/clscraper'

    start_docker = docker_operator.DockerOperator(
        api_version='auto',
        image='rental-housing-project-scraper',  # latest is used with tag is omitted
        command=docker_cmd,
        network_mode='bridge',
        auto_remove=True,
        task_id='task_docker'
    )

    start_docker