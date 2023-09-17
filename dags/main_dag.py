"""DAG to retrieve supermarket data from the Coles website"""
import pendulum
import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

# Some imports are imported within the DAG itself
# as per guidance on
# https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html

with DAG(
    dag_id="main_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 9, 12, tz="Australia/Perth"),
    catchup=False,
    tags=["main_dag"]):

    @task
    def clear_mongo_staging():
        from dag_scripts import clear_mongodb_staging
        return PythonOperator(task_id="clear_mongo_staging",
                              python_callable=clear_mongodb_staging.main)

    @task
    def website_to_mongo():
        from dag_scripts import website_to_mongodb
        return PythonOperator(task_id="website_to_mongo",
                              python_callable=website_to_mongodb.main)
    
    @task
    def mongo_to_postgres():
        from dag_scripts import mongodb_to_postgres
        return PythonOperator(task_id="mongo_to_postgres",
                              python_callable=mongodb_to_postgres.main)
    
    @task
    def run_dbt():
        return BashOperator(task_id="run_dbt",
                            bash_command=f"dbt run --profiles-dir /.dbt --project-dir /opt/airflow/dbt_project")
    
    @task
    def posgressql_to_redis():
        from dag_scripts import postgres_to_redis
        return PythonOperator(task_id="postgres_to_redis",
                              python_callable=postgres_to_redis.main)
    
    ############ MAIN DAG EXECUTION ############
    clear_mongo_staging()>> \
        website_to_mongo()>> \
            mongo_to_postgres()>>\
                  run_dbt()>> \
                    posgressql_to_redis()


    
