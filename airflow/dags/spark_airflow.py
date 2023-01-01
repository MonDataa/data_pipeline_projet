from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
#from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
#import time
import os
#import requests


def debut():
	os.popen('/opt/spark//bin/spark-submit /home/momo/Bureau/spark_start.py')
	pass

def data_ingestion_nifi_start():
	os.popen('/home/momo/Bureau/nifi-1.18.0-bin/nifi-1.18.0/bin/nifi.sh start')
	pass
"""
def data_procession_spark():
	os.popen('')
	pass
"""
def stop_spark():
	os.popen('/opt/spark//bin/spark-submit /home/momo/Bureau/spark_stop.py')
	pass

def nifi_stop():
	os.popen('/home/momo/Bureau/nifi-1.18.0-bin/nifi-1.18.0/bin/nifi.sh stop')
	pass


args = {
		'owner': 'Ranga',
		'start_date': datetime(2022, 12, 28),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

with DAG(
	dag_id='spark_airflow_project',
	default_args=args,
	description='Orchestration de spark avec airlfow',
	schedule_interval='0 * * * *', 
	catchup=False,
	tags=['project'],
) as dag:

	start_spark_ = PythonOperator(
	task_id="start_spark_data",
	python_callable=debut
	)

	start_nifi = PythonOperator(
	task_id="start_nifi_process",
	python_callable=data_ingestion_nifi_start,
	)


	python_submit_job = SparkSubmitOperator(
	application="/home/momo/Bureau/spark_d2.py",task_id="spark_job_process"
	)

	stop_spark_ = PythonOperator(
			task_id="stop_spark_process",
			python_callable=stop_spark
		)

	stop_nifi=PythonOperator(
			task_id="stop_nifi_process",
			python_callable=nifi_stop,
		)


start_spark_ >> start_nifi >> python_submit_job >> stop_spark_ >> stop_nifi
