# INE Callejero Source
#
# The Source has no runtime dependency: PYTHONPATH=src is enough to run everything.
# The venv targets exist to build/package, not to run.
#
# No API: "extract" ingests files already downloaded manually from the INE website into IN.

PYTHON      ?= python3
VENV        ?= venv
VENV_PY      = $(VENV)/bin/python
export PYTHONPATH = src

IN          ?= ../../temp
ROOT        ?= data/callejero
PROVINCES   ?= 08,28,41,46
DATE        ?= $(shell date -u +%F)
PARTITION    = $(ROOT)/ingestion_date=$(DATE)

.PHONY: help test extract validate check venv install freeze clean-pycache

help:
	@echo "test      - run the suite (no network, no dependencies)"
	@echo "extract   - ingest the files from IN=$(IN) into $(PARTITION)"
	@echo "validate  - validate $(PARTITION) in --strict mode"
	@echo "check     - test + validate"
	@echo ""
	@echo "venv      - create $(VENV) and install the build tools"
	@echo "install   - install the package in editable mode inside $(VENV)"
	@echo "freeze    - rewrite requirements-build.txt from $(VENV)"
	@echo ""
	@echo "variables: IN=$(IN) ROOT=$(ROOT) PROVINCES=$(PROVINCES) DATE=$(DATE) VENV=$(VENV)"

test:
	$(PYTHON) -m unittest discover -s tests -t .

extract:
	$(PYTHON) -m ine_callejero_source extract --in $(IN) --out $(ROOT) --provinces $(PROVINCES) --date $(DATE)

validate:
	$(PYTHON) -m ine_callejero_source validate $(PARTITION) --strict

check: test validate

venv:
	$(PYTHON) -m venv $(VENV)
	$(VENV_PY) -m pip install --quiet --upgrade pip
	$(VENV_PY) -m pip install --quiet -r requirements-build.txt

install: venv
	$(VENV_PY) -m pip install --quiet -e .
	$(VENV)/bin/ine-callejero-source --version

# Records only the build tools. The package itself and pip are excluded: the former is
# not on PyPI and would break `pip install -r`; the latter already ships with the venv.
freeze:
	@printf '%s\n' \
	  '# BUILD/PACKAGING tools. Not runtime dependencies.' \
	  '#' \
	  '# Needed only for `pip install -e .` / `pip install .` / building the wheel.' \
	  '# A container that runs the Source via PYTHONPATH=src needs none of this.' \
	  '#' \
	  '# Versions pinned from the environment where the package was built and validated' \
	  '('$(VENV)'/, Python '"$$($(VENV_PY) -c 'import platform;print(platform.python_version())')"'), obtained with `pip freeze`.' \
	  '' > requirements-build.txt
	@$(VENV_PY) -m pip freeze --exclude-editable \
	  | grep -v -e '^pip==' -e '^ine-callejero-source' -e '^-e ' >> requirements-build.txt
	@echo "requirements-build.txt rewritten:"; grep -v '^#' requirements-build.txt | grep .

clean-pycache:
	find . -name '__pycache__' -type d -prune -not -path './$(VENV)/*' -exec rm -rf {} +
