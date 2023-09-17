FROM apache/airflow:2.7.1
COPY requirements.txt /

# This project uses the psycopg2-binary library in Python
# But the following can be uncommented to use with full PostgreSQL features
#USER root
#RUN sudo apt-get update
#RUN sudo apt-get install -y libpq-dev libssl-dev postgresql-doc-15 libssl-doc gcc

USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt