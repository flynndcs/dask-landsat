install:
	@python3 -m venv venv
	@source venv/bin/activate; \
	pip install -r requirements.txt

.PHONY: install

format:
	@echo "Black:"
	@black .
	@echo "Flake8:"
	@flake8 .

.PHONY: format 

run:
	@echo "Running dask-landsat"
	@echo "--------------------"
	@python3 main.py

.PHONY: run