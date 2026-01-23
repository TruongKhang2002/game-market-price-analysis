from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sys
import os

# Import function từ scripts
# Cần thêm đường dẫn scripts vào sys.path để airflow tìm thấy module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/../scripts"))
from ingest_data import ingest_data_to_minio
from load_to_postgres import load_data_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'game_price_analytics',
    default_args=default_args,
    description='ETL pipeline for Game Prices',
    schedule_interval='0 9 * * *', # Chạy 9h sáng hàng ngày
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bigdata'],
) as dag:

    # Task 1: Ingest Data (Python)
    t1_ingest = PythonOperator(
        task_id='ingest_api_to_minio',
        python_callable=ingest_data_to_minio,
        op_kwargs={'execution_date': '{{ ds }}'}, # {{ ds }} là biến ngày của Airflow (YYYY-MM-DD)
    )

    # Task 2: Transform Data (Spark Submit)
    # Lưu ý: Spark chạy trong container riêng, nên đường dẫn script phải là đường dẫn trong container Spark
    t2_transform = SparkSubmitOperator(
        task_id='transform_with_spark',
        conn_id='spark_default', # Cần cấu hình connection này trong Airflow UI hoặc để mặc định nếu chạy local
        application='/opt/airflow/scripts/transform_data.py', # Đường dẫn đã mount trong docker-compose
        # Cấu hình master/worker mapping
        jars='/opt/airflow/jars-custom/hadoop-aws-3.3.4.jar,/opt/airflow/jars-custom/aws-java-sdk-bundle-1.12.262.jar',
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.driver.extraClassPath": "/opt/airflow/jars-custom/hadoop-aws-3.3.4.jar:/opt/airflow/jars-custom/aws-java-sdk-bundle-1.12.262.jar",
            "spark.executor.extraClassPath": "/opt/airflow/jars-custom/hadoop-aws-3.3.4.jar:/opt/airflow/jars-custom/aws-java-sdk-bundle-1.12.262.jar"
        }
    )

    # Task 3: Load to DW (Python)
    t3_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_data_to_postgres,
    )

    # Định nghĩa thứ tự chạy
    t1_ingest >> t2_transform >> t3_load