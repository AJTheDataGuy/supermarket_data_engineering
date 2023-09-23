"""DAG for running tests"""
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
    dag_id="tests_dag",
    schedule='@daily',
    start_date=pendulum.datetime(2023, 9, 12, tz="Australia/Perth"),
    catchup=False,
    tags=["tests_dag"]):

    def run_test(task_id="",
                     test_dir="/opt/airflow/dags/dag_scripts/tests",
                     test_name=""):
        log_name = test_name.replace("py","log")
        return BashOperator(task_id=task_id,
                            bash_command=f"pytest {test_dir}/{test_name} > {test_dir}/{log_name}")
    
    ############ MAIN DAG EXECUTION ############
    run_test(task_id="test_imports",
             test_name="test_imports.py")>> \
             \
        run_test(task_id="test_db_conns",
             test_name="test_db_connections.py")>> \
             \
            [run_test(task_id="test_mongodb_clear",
             test_name="test_mongodb_clear.py"),
             \
             run_test(task_id="test_website_to_mongo",
             test_name="test_website_to_mongo.py"),
             \
             run_test(task_id="test_mongodb_to_postgres",
             test_name="test_mongodb_to_postgres.py"),
             \
             run_test(task_id="test_postgres_to_redis",
             test_name="test_postgres_to_redis.py")]
             



    
