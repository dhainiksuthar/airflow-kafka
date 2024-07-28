FROM apache/airflow:2.9.3  

USER airflow
RUN pip install kafka-python
RUN pip install kafka-python-ng
