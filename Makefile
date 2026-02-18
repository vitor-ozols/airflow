COMPOSE := docker compose

.PHONY: prepare init up down restart logs ps clean

prepare:
	mkdir -p dags logs plugins
	chmod -R a+rwX dags logs plugins 2>/dev/null || true

init: prepare
	$(COMPOSE) run --rm airflow-init

up: init
	$(COMPOSE) up -d airflow-api-server airflow-scheduler airflow-dag-processor

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f airflow-api-server airflow-scheduler airflow-dag-processor

ps:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down -v --remove-orphans

pass:
	$(COMPOSE) exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated