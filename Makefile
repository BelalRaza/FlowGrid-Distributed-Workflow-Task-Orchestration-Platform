# Docker Compose's default "bake" builder fails when the project path contains
# non-ASCII characters (this folder name has an em dash), so use the classic
# compose build path.
export COMPOSE_BAKE=false

.PHONY: up down logs ps test test-pg smoke worker-scale clean

up:            ## Build and start postgres, redis, migrations, api, worker
	docker compose up -d --build

down:          ## Stop the stack (keeps the database volume)
	docker compose down

clean:         ## Stop the stack and delete the database volume
	docker compose down -v

logs:          ## Follow api + worker logs
	docker compose logs -f api worker

ps:
	docker compose ps

test:          ## Run the test suite locally (SQLite, no services needed)
	.venv/bin/pytest

test-pg:       ## Run the test suite inside Docker against PostgreSQL
	docker compose exec -T postgres psql -U flowgrid -tc "SELECT 1 FROM pg_database WHERE datname='flowgrid_test'" | grep -q 1 \
		|| docker compose exec -T postgres psql -U flowgrid -c "CREATE DATABASE flowgrid_test"
	docker compose run --rm -e TEST_DATABASE_URL=postgresql+psycopg://flowgrid:flowgrid@postgres:5432/flowgrid_test api pytest

smoke:         ## End-to-end check against the running stack (real Redis + worker)
	.venv/bin/python scripts/smoke_test.py
