FROM apache/airflow:3.0.2

USER root
ENV UV_COMPILE_BYTECODE=1

COPY pyproject.toml pylock.toml ./
RUN uv pip install -r pylock.toml --system

# Return back to the unprivileged user
USER airflow
