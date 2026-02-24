COMPOSE := docker compose
AIRFLOW_SERVICES ?= airflow-api-server airflow-scheduler airflow-dag-processor

.PHONY: prepare init up down restart restart-airflow logs ps clean

prepare:
	mkdir -p dags logs plugins
	chmod -R a+rwX dags logs plugins 2>/dev/null || true

init: prepare
	$(COMPOSE) run --rm airflow-init

up: init
	$(COMPOSE) up -d $(AIRFLOW_SERVICES)

down:
	$(COMPOSE) down

restart: down up
restart-airflow:
	$(COMPOSE) restart $(AIRFLOW_SERVICES)

logs:
	$(COMPOSE) logs -f $(AIRFLOW_SERVICES)

ps:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down -v --remove-orphans

pass:
	$(COMPOSE) exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated
