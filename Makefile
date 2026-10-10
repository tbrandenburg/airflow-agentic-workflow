COMPOSE = docker compose --env-file infra/airflow/.local/compose.env -f infra/airflow/compose.yaml
PYTHON = .venv/bin/python

.PHONY: install run stop test lint build clean format-check typecheck smoke status smoke-p2 schemas schemas-check test-storage
install:
	npm ci --prefix packages/agent-runtime
	npm run build --prefix packages/agent-runtime
	uv venv --python 3.12 --allow-existing
	uv pip install --python .venv/bin/python -r pyproject.toml --group dev
	uv pip install --python .venv/bin/python -e ./airflow-core --no-deps
	$(PYTHON) tools/deploy.py bootstrap
build: install
	$(COMPOSE) build
run: install
	$(PYTHON) tools/deploy.py preflight
	$(COMPOSE) up -d --build --wait --wait-timeout 420
stop:
	$(COMPOSE) down --timeout 30
status:
	$(COMPOSE) ps -a
test:
	npm test --prefix packages/agent-runtime
	$(PYTHON) tools/schemas.py
	$(PYTHON) -m pytest -q
lint:
	npm run lint --prefix packages/agent-runtime
	.venv/bin/ruff check .
format-check:
	npm run format:check --prefix packages/agent-runtime
	.venv/bin/ruff format --check .
typecheck:
	npm run typecheck --prefix packages/agent-runtime
	.venv/bin/mypy
smoke:
	$(PYTHON) tools/smoke.py
smoke-p2:
	$(PYTHON) tools/smoke_p2.py
smoke-p3:
	PYTHONPATH=services/run-api:airflow-core $(PYTHON) tools/smoke_p3.py
smoke-p4:
	PYTHONPATH=services/run-api:airflow-core $(PYTHON) tools/smoke_p4.py
smoke-p5:
	PYTHONPATH=services/run-api:airflow-core $(PYTHON) tools/smoke_p5.py
acceptance:
	PYTHONPATH=services/run-api:airflow-core $(PYTHON) tools/acceptance.py
audit:
	npm audit --prefix packages/agent-runtime
shellcheck:
	shellcheck projects/example-service/scripts/*.sh
schemas:
	$(PYTHON) tools/schemas.py --write
schemas-check:
	$(PYTHON) tools/schemas.py
test-storage:
	$(PYTHON) -m pytest -q tests/integration
clean: stop
	$(PYTHON) tools/deploy.py clean
