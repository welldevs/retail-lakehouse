"""O portao do build do Silver — decidido num lugar so, e verificado sem rede.

POR QUE ESTA SUITE EXISTE, e ela existe por um defeito que passou tres dias em producao.

A decisao de "o que excluir do `dbt build`" vivia em SEIS arquivos: o alvo `silver` do
Makefile, com os cinco portoes, e cada uma das cinco DAGs, com o portao da propria source e
mais nenhum. Nenhuma DAG tinha o portao do Iceberg, e a da Mercadona nao tinha portao algum.

O sintoma: `mercadona_catalog_daily` reprovava TODO DIA na ultima tarefa. Os quatro armazens
extraiam, validavam, aterrissavam e verificavam com sucesso; o `silver` caia com
"no version-hint could be found". O dado estava certo o tempo inteiro — o portao e que
morava no arquivo errado.

Nada pegou isso porque nao havia o que pegar: `make silver` passava (tem o portao completo),
`make test` passava (nao olha DAG), e a suite do dbt nunca chegava a rodar. So a execucao
real reprovava, e um dia depois.

Estes testes miram nos dois lados do defeito:

  * `plan()` e PURA, entao a decisao inteira e exercitavel sem MinIO e sem catalogo;
  * e ha uma checagem de FONTE que exige que toda DAG que constroi o Silver passe pelo
    verbo, em vez de montar o proprio comando de dbt. Um portao correto em um lugar so vale
    enquanto for o unico lugar.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.silver_gate import (  # noqa: E402
    ICEBERG_NODES,
    ICEBERG_VAR,
    SOURCE_MODELS,
    plan,
)

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DAGS = os.path.join(REPO, "orchestration", "airflow", "dags")
MODELS = os.path.join(REPO, "platform", "dbt", "models", "silver")

TUDO = {prefixo: True for prefixo, _ in SOURCE_MODELS}
METADADO = "s3://retail-lakehouse/iceberg/projection/live_order_state/metadata/00052-x.json"


class PlanoTest(unittest.TestCase):
    def test_com_tudo_no_lugar_nao_exclui_nada(self):
        self.assertEqual(plan(TUDO, METADADO)[:1], ["--vars"])
        self.assertNotIn("--exclude", plan(TUDO, METADADO))

    def test_source_sem_dado_leva_todos_os_seus_modelos_junto(self):
        """Excluir o modelo e deixar o irmao que faz `ref()` nele seria trocar um erro por
        outro: `silver_price_change` le `silver_product_price`, e `silver_order`/
        `silver_order_line` leem `silver_order_event`."""
        landed = dict(TUDO, mercadona_catalog_api=False)
        argumentos = plan(landed, METADADO)
        for modelo in ("silver_product_price", "silver_price_change", "silver_category",
                       "raw_manifest"):
            self.assertIn(modelo, argumentos)

    def test_catalogo_indisponivel_exclui_o_modelo_E_o_teste_que_o_compara(self):
        """Excluir so o modelo deixaria o teste tentando `ref()` de algo que nao foi
        construido — um erro de compilacao no lugar de um erro de execucao."""
        argumentos = plan(TUDO, None)
        for no in ICEBERG_NODES:
            self.assertIn(no, argumentos)
        self.assertNotIn("--vars", argumentos)

    def test_este_e_o_defeito_de_2026_08_31(self):
        """A combinacao exata que reprovava: TODAS as sources com dado, catalogo fora.

        Era o estado normal de qualquer maquina sem `make stream-up`, e do container do
        Airflow, que ate entao nem tinha como alcancar o catalogo. O build tem de ficar
        VERDE com um modelo a menos, nao vermelho com um modelo impossivel.
        """
        argumentos = plan(TUDO, None)
        self.assertEqual(argumentos[0], "--exclude")
        self.assertEqual(set(argumentos[1:]), set(ICEBERG_NODES))

    def test_prefixo_ausente_do_dicionario_conta_como_nao_aterrissado(self):
        """O lado seguro: excluir um modelo que poderia ser construido custa uma execucao;
        tentar construir um que nao pode custa o build inteiro."""
        argumentos = plan({}, None)
        for _, modelos in SOURCE_MODELS:
            for modelo in modelos:
                self.assertIn(modelo, argumentos)

    def test_o_caminho_do_metadado_vai_entre_aspas_no_yaml(self):
        """`s3://...` sem aspas faz o YAML ler `s3:` como chave e o var chegar VAZIO — e um
        var vazio nao reprova o parse, reprova o modelo, tres passos adiante."""
        argumentos = plan(TUDO, METADADO)
        var = argumentos[argumentos.index("--vars") + 1]
        self.assertEqual(var, f'{{{ICEBERG_VAR}: "{METADADO}"}}')


class ModelosDeclaradosExistemTest(unittest.TestCase):
    def test_todo_modelo_citado_pelo_portao_existe_no_disco(self):
        """Um nome errado aqui nao reprova nada: o dbt ignora `--exclude` de um no
        inexistente, e o modelo que se queria excluir e construido assim mesmo."""
        no_disco = {
            os.path.splitext(arquivo)[0]
            for _, _, arquivos in os.walk(MODELS)
            for arquivo in arquivos if arquivo.endswith(".sql")
        }
        self.assertTrue(no_disco, f"nenhum modelo encontrado em {MODELS}")
        for _, modelos in SOURCE_MODELS:
            for modelo in modelos:
                self.assertIn(modelo, no_disco)
        self.assertIn(ICEBERG_NODES[0], no_disco)

    def test_todo_modelo_de_source_esta_atribuido_a_alguma_source(self):
        """O inverso, e e o que pega o modelo NOVO: quem cria um modelo de source e esquece
        de registra-lo aqui produz exatamente o defeito de 2026-08-31 — um modelo que o
        portao nao sabe excluir, num repositorio onde aquela source ainda nao aterrissou."""
        declarados = {m for _, modelos in SOURCE_MODELS for m in modelos} | set(ICEBERG_NODES)
        # Modelos direto sob silver/ sao passagens de seed: nao dependem de RAW nenhum.
        for raiz, _, arquivos in os.walk(MODELS):
            if os.path.abspath(raiz) == os.path.abspath(MODELS):
                continue
            for arquivo in arquivos:
                if not arquivo.endswith(".sql"):
                    continue
                nome = os.path.splitext(arquivo)[0]
                self.assertIn(nome, declarados,
                              f"{nome} nao esta em SOURCE_MODELS nem em ICEBERG_NODES")


class TodaDagPassaPeloPortaoTest(unittest.TestCase):
    """A checagem que teria evitado o defeito: um portao unico so vale se for o unico.

    Nao basta o portao estar certo — nada impede a proxima DAG de montar o proprio
    `dbt build`, que foi exatamente como as seis copias nasceram.
    """

    def _dags(self):
        for arquivo in sorted(os.listdir(DAGS)):
            if not arquivo.endswith(".py"):
                continue
            with open(os.path.join(DAGS, arquivo), encoding="utf-8") as handle:
                yield arquivo, handle.read()

    def test_nenhuma_dag_do_silver_monta_o_proprio_comando_de_dbt(self):
        for arquivo, fonte in self._dags():
            if arquivo == "warehouse_load.py":
                # A UNICA excecao legitima: ela roda `--target snowflake`, onde a arvore le
                # so `source()` sobre o STAGE e portao nenhum se aplica.
                continue
            self.assertNotIn('"build"', fonte,
                             f"{arquivo} monta o proprio `dbt build` em vez de usar o verbo")

    def test_toda_dag_com_tarefa_silver_chama_o_verbo(self):
        vistas = 0
        for arquivo, fonte in self._dags():
            arvore = ast.parse(fonte)
            tem_silver = any(
                isinstance(no, ast.FunctionDef) and no.name == "silver"
                for no in ast.walk(arvore)
            )
            if not tem_silver:
                continue
            vistas += 1
            self.assertIn("silver-build", fonte,
                          f"{arquivo} tem tarefa `silver` que nao passa pelo portao")
        self.assertEqual(vistas, 5, "esperava as cinco DAGs de source com tarefa `silver`")

    def test_o_makefile_tambem_passa_pelo_verbo(self):
        """O Makefile foi o lugar CERTO por mais tempo, e ainda assim precisa da checagem:
        o defeito nao foi ele estar errado, foi ele estar sozinho."""
        with open(os.path.join(REPO, "Makefile"), encoding="utf-8") as handle:
            makefile = handle.read()
        alvo = makefile.split("\nsilver:", 1)[1].split("\n\n", 1)[0]
        # Comentarios fora: o alvo DESCREVE o portao que perdeu, e a palavra `--exclude`
        # aparece nessa explicacao. O que nao pode voltar e o alvo MONTAR a lista.
        comandos = "\n".join(
            linha for linha in alvo.split("\n") if not linha.lstrip().startswith("#")
        )
        self.assertIn("silver-build", comandos)
        self.assertNotIn("--exclude", comandos)
        self.assertNotIn("--vars", comandos)
        self.assertNotIn("has-data", comandos)
        self.assertNotIn("iceberg-metadata", comandos)


if __name__ == "__main__":
    unittest.main()
