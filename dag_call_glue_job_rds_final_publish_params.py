from datetime import datetime
from airflow import DAG
from shared_modules.operators.glue_job_operator import GlueJobOperator

# DAG Details
glue_job_name = 'glue_elt_rds_to_snowflake'
ingestion_type = "api"
schedule_interval = "20 13 * * *"
dag_arguments = {
    "owner": "airflow",
    "retries": 0
}

staging_tables = [
'rds_table_1'
,'rds_table_2'
,'rds_table_3'
,'rds_table_4'
,'rds_table_5'
,'rds_table_6'
,'rds_table_7'
,'rds_table_8'
,'rds_table_9'
,'rds_table_10'
,'rds_table_11'
,'rds_table_12'
,'rds_table_13'
,'rds_table_14'
,'rds_table_15'
,'rds_table_16'
,'rds_table_17'
,'rds_table_18'
,'rds_table_19'
,'rds_table_20'
]

# DAG instantiation
with DAG(
    dag_id="dag_call_glue_elt_rds_to_snowflake",
    start_date=datetime(2022, 7,15),
    schedule_interval=schedule_interval,
    catchup=False,
    default_args=dag_arguments,
    ) as dag:
  
    for i in range(0, len(staging_tables), 5):
        staging_tables_batch = staging_tables[i:i+5]
        merge_tasks = []
    
        for staging_table in staging_tables_batch:       
            # Load data to Delta Lake and Athena
            merge_task = GlueJobOperator(
                job_name = glue_job_name,
                task_id = f'rds_to_snowflake_load_{staging_table}',
                table_list =[staging_table],
                script_args={
                '--config_s3_file_path' : '### Enter the s3 path of config json file which we got at step deploy glue job using sdk', #TODO UPDATE PATH
                '--extra-jars': '### Enter the s3 path of jar files like  s3://key_to_file/postgresql-42.2.25.jar,s3://key_to_file/spark-snowflake_2.12-2.11.0-spark_3.1.jar,s3://key_to_file/snowflake-jdbc-3.13.22.jar' #TODO UPDATE PATH
                },
                run_job_kwargs={'WorkerType':'G.1X','NumberOfWorkers':'3'}
            )
            merge_tasks.append(merge_task)

        for j in range(1, len(merge_tasks)):
            merge_tasks[j-1] >> merge_tasks[j]
