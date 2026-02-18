COMPOSE := docker compose

.PHONY: prepare init up down restart logs ps clean

prepare:
	mkdir -p dags logs plugins
	chmod -R a+rwX dags logs plugins 2>/dev/null || true

init: prepare
	$(COMPOSE) run --rm airflow-init

up: init
	$(COMPOSE) up -d airflow-webserver airflow-scheduler

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f airflow-webserver airflow-scheduler

ps:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down -v --remove-orphans
