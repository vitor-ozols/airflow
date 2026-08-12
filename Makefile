COMPOSE := sudo docker compose
AIRFLOW_SERVICES ?= airflow-api-server airflow-scheduler airflow-dag-processor
SIMPLE_AUTH_PASSWORD_FILE ?= /opt/airflow/config/simple_auth_manager_passwords.json

.PHONY: prepare init up down restart restart-airflow logs ps clean pass repair-layer repair-layer-prune reset-db

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

repair-layer:
	@test -n "$(LAYER)" || (echo 'Usage: make repair-layer LAYER=<sha256-from-docker-error>' >&2; exit 2)
	./scripts/repair_docker_layerdb.sh "$(LAYER)"

repair-layer-prune:
	@test -n "$(LAYER)" || (echo 'Usage: make repair-layer-prune LAYER=<sha256-from-docker-error>' >&2; exit 2)
	./scripts/repair_docker_layerdb.sh --prune-build-cache "$(LAYER)"

reset-db:
	$(COMPOSE) down -v --remove-orphans
	$(MAKE) up

pass:
	@$(COMPOSE) exec -T airflow-api-server sh -lc 'i=0; while [ $$i -lt 30 ]; do if [ -s "$(SIMPLE_AUTH_PASSWORD_FILE)" ]; then cat "$(SIMPLE_AUTH_PASSWORD_FILE)"; exit 0; fi; i=$$((i + 1)); sleep 1; done; echo "Password file not found: $(SIMPLE_AUTH_PASSWORD_FILE)" >&2; exit 1'
