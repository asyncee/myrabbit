bootstrap:
	rm -rf ./env
	virtualenv -p python3 env
	env/bin/pip install -U pip
	env/bin/pip install poetry
	env/bin/poetry install

test:
	pytest --ff tests

benchmark:
	pytest -s -m benchmark tests

test_dev:
	MYRABBIT_DONT_CLEAR_VHOST=1 pytest --ff tests
