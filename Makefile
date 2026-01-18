SHELL := /bin/sh

.PHONY: up fixtures stage test test-one logs readme-check clean

up:
	docker compose down -v
	docker compose up -d --build

stage:
	bash scripts/stage_fixtures.sh

fixtures:
	mkdir -p _purple_output
	@if [ "$$CI" = "true" ]; then sudo chown -R $$(id -u):$$(id -g) _purple_output || true; fi
	python3 gen_fixtures.py
	bash scripts/stage_fixtures.sh

test:
	bash scripts/run_all.sh

test-one:
	bash scripts/run_one.sh $(TASK)

purple-one:
	python3 -m baseline_purple.run --task-id $(TASK)

purple-all:
	@for task in T1_single_page T2_multi_page T3_duplicates T4_rate_limit_429 T5_server_error_500 T6_page_drift T7_totals_trap; do \
		echo "Running baseline purple for $$task..."; \
		python3 -m baseline_purple.run --task-id $$task || exit 1; \
	done
	@echo "All tasks complete"


logs:
	docker compose logs -f --tail=200

readme-check:
	python3 -c "import pathlib; p=pathlib.Path('README.md').resolve(); print(p)"
	head -n 5 README.md
	wc -l README.md

clean:
	docker compose down -v
