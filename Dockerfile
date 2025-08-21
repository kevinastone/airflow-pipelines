FROM apache/airflow:3.0.2

ENV UV_COMPILE_BYTECODE=1

COPY pyproject.toml uv.lock ./
RUN uv sync --active --locked --no-editable
