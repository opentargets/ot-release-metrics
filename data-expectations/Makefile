# Makefile for Great Expectations workflow management

PROJECT_ID ?= open-targets-eu-dev
DB_ID ?= platform_dev

.PHONY: help setup add-expectations run-validations clean

help:
	@echo "Available commands:"
	@echo "  make setup              - Initialize GX context and configure BigQuery data source and assets"
	@echo "  make add-expectations   - Add expectations from the expectations directory"
	@echo "  make run-validations   - Run all configured validations"
	@echo "  make clean             - Remove GX data context files"

setup:
	@echo "Setting up Great Expectations context..."
	python gx_context_setup.py

add-expectations:
	@echo "Adding expectations"
	python gx_expectations_update.py

run-validations:
	@echo "Running validations..."
	python gx_runner.py

clean:
	@echo "Cleaning up Great Expectations files..."
	rm -rf gx/great_expectations.yml
	rm -rf gx/validation_definitions
	rm -rf gx/expectations
	rm -rf gx/checkpoints