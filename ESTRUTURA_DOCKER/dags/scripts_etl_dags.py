from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Definir o DAG
dag = DAG(
    'pyspark_etl_dag',
    description='ETL com PySpark orquestrado pelo Airflow',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 12, 6),
    catchup=False,
)

# Definir o operador para executar o script PySpark
spark_task = SparkSubmitOperator(
    task_id='run_pyspark_etl',
    application='/opt/airflow/scripts/scripts_etl.py',
    conn_id='spark_default',
    conf={'spark.executor.memory': '2g', 'spark.driver.memory': '2g'},
    dag=dag,
)

spark_task
