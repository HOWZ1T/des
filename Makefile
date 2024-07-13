.PHONY: lint
lint:
	@black ./src ./tests
	@isort ./src ./tests
	@flake8 ./src ./tests
