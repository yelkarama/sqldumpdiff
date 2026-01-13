.PHONY: go java python bench docs

go:
	@./scripts/run-go.sh

java:
	@./scripts/run-java.sh

python:
	@./scripts/run-py.sh

bench:
	@./scripts/bench.sh

docs:
	@echo "Docs live in ./docs"
