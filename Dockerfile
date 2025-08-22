FROM apache/airflow:3.0.2

ENV UV_COMPILE_BYTECODE=1
ENV UV_PROJECT_ENVIRONMENT=/home/airflow/.local

RUN curl "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION%.*}.txt" > ./constraints.txt

COPY pyproject.toml ./
RUN uv pip install -r pyproject.toml --constraint constraints.txt

COPY dags/ /opt/airflow/dags/
