bootstrap:
	rm -rf ./env
	virtualenv -p python3 env
	env/bin/pip install -U pip
	env/bin/pip install poetry
	env/bin/poetry install

test:
	poetry run pytest --ff --cov=src --cov-report term-missing  -m "not benchmark" -m "not instrumentation" tests

test_instrumentation:
	poetry run pytest -s --ff -m "instrumentation" tests

benchmark:
	pytest -s -m benchmark tests

style:
	env/bin/black src
	cd src && ../env/bin/isort -y

lint:
	mypy src
	flake8 src

test_dev:
	MYRABBIT_DONT_CLEAR_VHOST=1 pytest --ff tests
