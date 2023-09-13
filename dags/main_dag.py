"""DAG to retrieve supermarket data from the Coles website"""
import pendulum
import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

# Some imports are imported within the DAG itself
# as per guidance on
# https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html

with DAG(
    dag_id="main_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 9, 12, tz="Australia/Perth"),
    catchup=False,
    tags=["main_dag"],
) as dag:
    ##### TASK DEFINITIONS ########

    @task(task_id="website_to_mongodb")
    def web_to_mongo():
        from dag_scripts import website_to_mongodb
        return PythonOperator(python_callable=website_to_mongodb.main)
    
    @task(task_id="mongodb_to_postgres")
    def mongo_to_postgres():
        from dag_scripts import mongodb_to_postgres
        return PythonOperator(python_callable=mongodb_to_postgres.main)
    
    @task(task_id="run_dbt")
    def run_dbt(profile_dir,project_dir):
        return BashOperator(bash_command=f"dbt run --profiles-dir {profile_dir} --project-dir {project_dir}")
    
    @task(task_id="postgres_to_redis")
    def posgressql_to_redis():
        from dag_scripts import postgres_to_redis
        return PythonOperator(python_callable=postgres_to_redis.main)
    
    ############ MAIN DAG EXECUTION ############
    web_to_mongo()>> mongo_to_postgres()>> run_dbt()>> posgressql_to_redis()


    
