FROM apache/airflow:3.1.7-python3.11

USER airflow
COPY pyproject.toml /opt/airflow/pyproject.toml
COPY poetry.lock /opt/airflow/poetry.lock
RUN pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root --only main
