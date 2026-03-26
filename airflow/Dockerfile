FROM apache/airflow:2.10.4-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gosu \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt
