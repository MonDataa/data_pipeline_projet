from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {
	'owner':'Airflow',
}

with DAG(
	dag_id='example_spark_operator',
	default_args=args,
	schedule_interval=None,
	start_date=days_ago(2),
	tags=['example'],
) as dag:

    python_submit_job = SparkSubmitOperator(
	application="/opt/spark/examples/src/main/python/pi.py",task_id="python_job"
    )
    scala_submit_job = SparkSubmitOperator(
	application="/home/momo/Téléchargements/airflow-spark-assembly-0.1.0-SNAPSHOT.jar",task_id="scala_job"
    )

python_submit_job >> scala_submit_job
