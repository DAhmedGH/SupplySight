ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11
ARG AIRFLOW_VERSION

USER airflow

COPY --chown=airflow:root airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

RUN pip check

# dbt and the project connector require versions that conflict with Airflow's
# runtime dependencies. Keep project CLIs in their own Python environment.
RUN python -m venv /home/airflow/project-venv \
    && /home/airflow/project-venv/bin/pip install --no-cache-dir \
        -r /tmp/airflow-requirements.txt \
    && /home/airflow/project-venv/bin/python -m pip check

ENV PATH="/home/airflow/project-venv/bin:${PATH}" \
    SUPPLYSIGHT_PYTHON="/home/airflow/project-venv/bin/python"

# DAG tasks run Python CLIs and dbt from this repository mount.
WORKDIR /opt/supplysight
