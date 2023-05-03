import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'dayookubanjo',
    'depends_on_past': False,
    'email': ['dayookubanjo@yahoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0 
}

spark_dag = DAG(
        dag_id = "ingest-data_dag",
        default_args=default_args,
        schedule_interval=None,	
        dagrun_timeout=timedelta(minutes=10000),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

Extract = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_ingest_data.py",
		conn_id= 'spark_local', 
		task_id='spark_ingest_submit_task', 
		dag=spark_dag,
        conf={'master':'spark://b89158def524:7077'} 
		)

Transform = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_transform_data.py",
		conn_id= 'spark_local', 
		task_id='spark_transform_task', 
		dag=spark_dag,
        conf={'master':'spark://b89158def524:7077'}  
		)

Model_Train = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_train_model.py",
		conn_id= 'spark_local', 
		task_id='spark_train_model_task', 
		dag=spark_dag,
        conf={'master':'spark://b89158def524:7077'}  
		)

Extract >> Transform >> Model_Train


# Extract = BashOperator(
# 		bash_command = "python3 /opt/airflow/dags/spark_ingest_data.py",
# 		task_id='spark_ingest_submit_task', 
# 		dag=spark_dag 
# 		)

# Transform = BashOperator(
# 		bash_command = "python3 /opt/airflow/dags/spark_transform_data.py",  
# 		task_id='spark_transform_task', 
# 		dag=spark_dag 
# 		)

# Model_Train = BashOperator(
# 		bash_command = "python3 /opt/airflow/dags/spark_train_model.py", 
# 		task_id='spark_train_model_task', 
# 		dag=spark_dag 
# 		)

# Extract >> Transform >> Model_Train