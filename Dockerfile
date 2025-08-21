FROM apache/airflow:3.0.2

ENV UV_COMPILE_BYTECODE=1
ENV UV_PROJECT_ENVIRONMENT=/home/airflow/.local

COPY pyproject.toml ./
RUN uv pip install -r pyproject.toml
