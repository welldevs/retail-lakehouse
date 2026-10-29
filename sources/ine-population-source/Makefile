# INE Population Source
#
# The Source has no runtime dependency: PYTHONPATH=src is enough to run everything.
# The venv targets exist to build/package, not to run.

PYTHON      ?= python3
VENV        ?= venv
VENV_PY      = $(VENV)/bin/python
export PYTHONPATH = src

ROOT        ?= data/ine
TABLES      ?= 31304
DATE        ?= $(shell date -u +%F)
PARTITION    = $(ROOT)/ingestion_date=$(DATE)

.PHONY: help test extract validate check venv install freeze clean-pycache

help:
	@echo "test      - run the suite (no network, no dependencies)"
	@echo "extract   - extract a snapshot into $(PARTITION)"
	@echo "validate  - validate $(PARTITION) in --strict mode"
	@echo "check     - test + validate"
	@echo ""
	@echo "venv      - create $(VENV) and install the build tools"
	@echo "install   - install the package in editable mode inside $(VENV)"
	@echo "freeze    - rewrite requirements-build.txt from $(VENV)"
	@echo ""
	@echo "variables: ROOT=$(ROOT) TABLES=$(TABLES) DATE=$(DATE) VENV=$(VENV)"

test:
	$(PYTHON) -m unittest discover -s tests -t .

extract:
	$(PYTHON) -m ine_population_source extract --out $(ROOT) --tables $(TABLES) --date $(DATE)

validate:
	$(PYTHON) -m ine_population_source validate $(PARTITION) --strict

check: test validate

venv:
	$(PYTHON) -m venv $(VENV)
	$(VENV_PY) -m pip install --quiet --upgrade pip
	$(VENV_PY) -m pip install --quiet -r requirements-build.txt

install: venv
	$(VENV_PY) -m pip install --quiet -e .
	$(VENV)/bin/ine-population-source --version

# Writes only the build tools. The package itself and pip are excluded: the former does
# not exist on PyPI and would break `pip install -r`; the latter already ships with the venv.
freeze:
	@printf '%s\n' \
	  '# BUILD/PACKAGING tools. These are not runtime dependencies.' \
	  '#' \
	  '# Needed only for `pip install -e .` / `pip install .` / building the wheel.' \
	  '# A container that runs the Source via PYTHONPATH=src needs none of this.' \
	  '#' \
	  '# Versions frozen from the environment where the package was built and validated' \
	  '# ('$(VENV)'/, Python '"$$($(VENV_PY) -c 'import platform;print(platform.python_version())')"'), obtained with `pip freeze`.' \
	  '' > requirements-build.txt
	@$(VENV_PY) -m pip freeze --exclude-editable \
	  | grep -v -e '^pip==' -e '^ine-population-source' -e '^-e ' >> requirements-build.txt
	@echo "requirements-build.txt rewritten:"; grep -v '^#' requirements-build.txt | grep .

clean-pycache:
	find . -name '__pycache__' -type d -prune -not -path './$(VENV)/*' -exec rm -rf {} +
