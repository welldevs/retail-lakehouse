# INE Callejero Source
#
# A Source nao tem dependencia de runtime: PYTHONPATH=src basta para rodar tudo.
# Os alvos de venv existem para construir/empacotar, nao para executar.
#
# Sem API: "extract" incorpora arquivos ja baixados manualmente do site do INE em IN.

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
	@echo "test      - roda a suite (sem rede, sem dependencias)"
	@echo "extract   - incorpora os arquivos de IN=$(IN) para $(PARTITION)"
	@echo "validate  - valida $(PARTITION) em modo --strict"
	@echo "check     - test + validate"
	@echo ""
	@echo "venv      - cria $(VENV) e instala as ferramentas de build"
	@echo "install   - instala o pacote em modo editavel dentro de $(VENV)"
	@echo "freeze    - regrava requirements-build.txt a partir de $(VENV)"
	@echo ""
	@echo "variaveis: IN=$(IN) ROOT=$(ROOT) PROVINCES=$(PROVINCES) DATE=$(DATE) VENV=$(VENV)"

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

# Grava apenas as ferramentas de build. O proprio pacote e o pip sao excluidos: o
# primeiro nao existe no PyPI e quebraria `pip install -r`; o segundo ja vem no venv.
freeze:
	@printf '%s\n' \
	  '# Ferramentas de BUILD/EMPACOTAMENTO. Nao sao dependencias de runtime.' \
	  '#' \
	  '# Necessarias apenas para `pip install -e .` / `pip install .` / construir a wheel.' \
	  '# Um container que rode a Source via PYTHONPATH=src nao precisa de nada disto.' \
	  '#' \
	  '# Versoes congeladas do ambiente onde o pacote foi construido e validado' \
	  '('$(VENV)'/, Python '"$$($(VENV_PY) -c 'import platform;print(platform.python_version())')"'), obtidas com `pip freeze`.' \
	  '' > requirements-build.txt
	@$(VENV_PY) -m pip freeze --exclude-editable \
	  | grep -v -e '^pip==' -e '^ine-callejero-source' -e '^-e ' >> requirements-build.txt
	@echo "requirements-build.txt regravado:"; grep -v '^#' requirements-build.txt | grep .

clean-pycache:
	find . -name '__pycache__' -type d -prune -not -path './$(VENV)/*' -exec rm -rf {} +
