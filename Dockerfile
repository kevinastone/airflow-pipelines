FROM apache/airflow:3.0.5

COPY pyproject.toml uv.lock ./
RUN uv sync --locked
