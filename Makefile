bootstrap:
	rm -rf ./env
	virtualenv -p python3 env
	env/bin/pip install -U pip
	env/bin/pip install poetry
	env/bin/poetry install

test:
	pytest --ff --cov=src --cov-report term-missing  -m "not benchmark" tests

benchmark:
	pytest -s -m benchmark tests

lint:
	mypy src
	flake8 src

test_dev:
	MYRABBIT_DONT_CLEAR_VHOST=1 pytest --ff tests
