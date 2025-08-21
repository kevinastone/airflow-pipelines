FROM apache/airflow:3.0.2

COPY pyproject.toml uv.lock ./
RUN uv sync --locked
