from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bdl_a2_2_function import *

# Define DAG parameters
default_args = {
    "owner": "airflow",
    'depends_on_past': False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="bdl_data_pipeline_a2_2",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # Wait for data archive
    wait_for_data = FileSensor(
        task_id="wait_for_data",
        filepath=f"{data_dir}/{zip_file_name}",
        fs_conn_id = 'bdl_a2_sensor',
        timeout=5,
    )

    # Unzip archive
    unzip_data = BashOperator(
        task_id="unzip_data",
        bash_command=f"mkdir {data_dir}/csv_files ; unzip -q {data_dir}/{zip_file_name} -d {data_dir}/csv_files",
    )

    output_path = 'output/processed'
    beam_process = PythonOperator(
        task_id='data_processing_pipeline',
        python_callable=beam_pipeline,
        op_kwargs={'data_cols': required_fields, 'output_path': output_path},
        dag=dag,
    )

    heatmap_fields = ['HourlyDewPointTemperature', 'HourlyDryBulbTemperature', 'HourlyPrecipitation']

    make_plot = PythonOperator(
        task_id='data_visualisation_pipeline',
        python_callable=geopandas_visualisation,
        op_kwargs={'heatmap_fields': heatmap_fields},
        dag=dag,
    )

    delete_data = BashOperator(
        task_id="deleting_csvs",
        bash_command=f'rm -r {data_dir}/csv_files/; rm -r /home/parallels/Downloads/BDl/output/',
        dag=dag
    )

    # Set task dependencies
    wait_for_data >> unzip_data >> beam_process >> make_plot >> delete_data
