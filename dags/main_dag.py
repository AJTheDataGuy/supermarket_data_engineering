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
    schedule='@weekly',
    start_date=pendulum.datetime(2023, 9, 12, tz="Australia/Perth"),
    catchup=False,
    tags=["main_dag"]):

    
    def clear_mongo_staging(task_id="clear_mongodb_staging"):
        from dag_scripts import clear_mongodb_staging
        return PythonOperator(task_id=task_id,
                              python_callable=clear_mongodb_staging.main)

    
    def website_to_mongo(task_id="website_to_mongo"):
        from dag_scripts import website_to_mongodb
        return PythonOperator(task_id=task_id,
                              python_callable=website_to_mongodb.main)
    
    
    def mongo_to_postgres(task_id="mongo_to_postgres"):
        from dag_scripts import mongodb_to_postgres
        return PythonOperator(task_id=task_id,
                              python_callable=mongodb_to_postgres.main)
    
    def install_dbt_deps(task_id="install_dbt_deps",
                profiles_dir="/.dbt",
                project_dir="/dbt_project"):
        return BashOperator(task_id=task_id,
                            bash_command=f"dbt deps --profiles-dir {profiles_dir} --project-dir {project_dir}")
    
    def run_dbt(task_id="run_dbt",
                profiles_dir="/.dbt",
                project_dir="/dbt_project"):
        return BashOperator(task_id=task_id,
                            bash_command=f"dbt run --profiles-dir {profiles_dir} --project-dir {project_dir}")

    def dbt_test(task_id="dbt_run_tests",
                profiles_dir="/.dbt",
                project_dir="/dbt_project"):
        return BashOperator(task_id=task_id,
                            bash_command=f"dbt test --profiles-dir {profiles_dir} --project-dir {project_dir}")
    
    def posgressql_to_redis(task_id="postgres_to_redis"):
        from dag_scripts import postgres_to_redis
        return PythonOperator(task_id=task_id,
                              python_callable=postgres_to_redis.main)
    
    ############ MAIN DAG EXECUTION ############
    clear_mongo_staging()>> \
        website_to_mongo()>> \
            mongo_to_postgres()>>\
                install_dbt_deps()>>\
                    run_dbt()>> \
                        dbt_test()>>\
                            posgressql_to_redis()


    
