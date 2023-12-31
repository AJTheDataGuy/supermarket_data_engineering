FROM apache/airflow:2.7.1
COPY requirements.txt /
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt