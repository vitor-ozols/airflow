FROM apache/airflow:2.10.5-python3.11

USER airflow
COPY pyproject.toml /opt/airflow/pyproject.toml
RUN pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root --only main
