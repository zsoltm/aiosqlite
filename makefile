.venv:
	python -m venv .venv
	source .venv/bin/activate && make setup dev
	echo 'run `source .venv/bin/activate` to develop aiosqlite'

venv: .venv

setup:
	python -m pip install -Ur requirements-dev.txt

dev:
	flit install --symlink

release: lint test clean
	flit publish

format:
	python -m isort --apply --recursive aiosqlite
	python -m black aiosqlite

lint:
	python -m pylint --rcfile .pylint aiosqlite/*.py
	python -m isort --diff aiosqlite
	python -m black --check aiosqlite

test:
	python -m coverage run -m aiosqlite.tests
	python -m coverage report
	python -m mypy aiosqlite/*.py

perf:
	python -m unittest -v aiosqlite.tests.perf

html: .venv README.rst docs/*.rst docs/conf.py
	source .venv/bin/activate && sphinx-build -b html docs html

clean:
	rm -rf build dist html README MANIFEST *.egg-info

distclean: clean
	rm -rf .venv
