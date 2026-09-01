"""A ponte entre o Silver e o job de estoque em Spark, nos dois sentidos.

    make stock-consumption      Silver  -> operations.stock_consumption   (pyiceberg)
    make stock-ledger           consumo -> operations.stock_ledger        (Spark)
    make silver                 ledger  -> silver_stock_ledger            (DuckDB)

POR QUE O SPARK NAO LE O PARQUET DO SILVER DIRETO, E ISSO E UMA DECISAO E NAO UM DESVIO.

Ler `s3://retail-lakehouse/silver/silver_order_line.parquet` do Spark exigiria o
`hadoop-aws` mais o bundle do AWS SDK v1 na imagem — cerca de 200 MB de jar e uma segunda
integracao com o object storage, com o proprio conjunto de modos de falha. O Spark ja fala
com o MinIO por um caminho: o `S3FileIO` do Iceberg, que o Marco A mediu.

Entao o consumo atravessa como TABELA ICEBERG. O ganho nao e so o jar que nao entra: a
mesma propriedade que justificou o Iceberg — interop entre engines — passa a ser exercida
pelo caminho de producao, e nao apenas por um experimento. O trajeto completo tem tres
motores e duas linguagens:

    DuckDB (Silver)  ->  pyiceberg escreve  ->  Spark le, calcula, escreve  ->  DuckDB le

O CONSUMO E OBSERVADO, NAO PREMISSA. Ele sai de `silver_order_line`, que sai do log de
eventos. As premissas de estoque (`stock_premises_seed`) dizem quantos DIAS cobrir; quantas
unidades isso vale por serie e consequencia do que os pedidos consumiram.

TRES DECISOES DE SEMANTICA que mudam o numero e por isso ficam escritas:

  1. CONSOME O PRODUTO ENTREGUE, nao o pedido. Uma linha substituida tira do estoque do
     SUBSTITUTO — por isso a chave e `fulfilled_source_product_id`. Agregar pelo produto
     original creditaria consumo a uma prateleira que ninguem tocou, e o total continuaria
     fechando.
  2. CONSOME NA SEPARACAO, nao na colocacao. O instante e `picked_at`, do pedido, e nao a
     particao do dia. Um pedido colocado as 23h e separado na madrugada seguinte tira do
     estoque do dia seguinte. Usar a particao seria mais simples e estaria errado nas
     bordas — e as bordas sao justamente onde a ruptura aparece.
  3. LINHA REMOVIDA NAO CONSOME. `line_status = 'removed'` e a linha que a separacao nao
     achou; ela nao sai da prateleira. E `not_picked` tambem nao: sao pedidos cancelados ou
     recusados no pagamento, que nunca chegaram a separacao.

O LIMITE QUE ISSO DEIXA, DECLARADO. `removal_rate` do gerador produz linhas com motivo
"unavailable" a uma taxa FIXA, sorteada, sem olhar saldo nenhum. A ruptura que este ledger
calcula e independente dela: uma nao causa a outra. Faze-las coerentes exigiria o gerador
LER o ledger, e isso inverteria a dependencia do projeto inteiro — hoje pedido gera estoque,
e passaria a haver um ciclo. Gatilho para mudar: um gerador de segunda passada, que releia o
saldo do dia anterior. Nao esta no escopo, e a alternativa (fingir que a taxa fixa e ruptura)
seria pior que a ausencia.
"""

from __future__ import annotations

import os

NAMESPACE = os.environ.get("STOCK_NAMESPACE", "operations")
CONSUMPTION_TABLE = f"{NAMESPACE}.stock_consumption"
LEDGER_TABLE = f"{NAMESPACE}.stock_ledger"


class StockLedgerError(RuntimeError):
    """Falha ao mover o consumo ou o ledger entre o Silver e o catalogo."""


def catalog(config=None):
    """O MESMO catalogo da projecao viva. Nao ha um segundo.

    Um catalogo por dominio pareceria mais organizado e criaria dois lugares onde uma
    tabela pode estar — e o `silver_gate` teria de perguntar aos dois. O namespace ja
    separa `projection` de `operations`.
    """
    from .orders_projection import catalog as abrir

    return abrir(config)


def consumption_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("wh", pa.string(), nullable=False),
        pa.field("source_product_id", pa.string(), nullable=False),
        pa.field("stock_date", pa.date32(), nullable=False),
        pa.field("units_consumed", pa.int64(), nullable=False),
        # PROVENIENCIA, como em live_order_state: sem ela, "quem escreveu esta linha" seria
        # afirmacao sobre log em vez de fato consultavel.
        pa.field("written_by", pa.string(), nullable=False),
    ])


CONSUMO_SQL = """
    select
        l.wh                                    as wh,
        l.fulfilled_source_product_id           as source_product_id,
        cast(o.picked_at as date)               as stock_date,
        cast(sum(l.quantity) as bigint)         as units_consumed
    from silver_order_line l
    join silver_order o using (order_id)
    where o.picked_at is not null
      and l.line_status in ('fulfilled', 'substituted')
      and l.fulfilled_source_product_id is not null
    group by 1, 2, 3
"""


def export_consumption(config=None, *, cat=None) -> dict:
    """Le o consumo do Silver e o (re)escreve inteiro em `operations.stock_consumption`.

    SOBRESCRITA TOTAL, e nao append: esta tabela e uma PROJECAO do Silver, nao um log. Se
    a janela de pedidos for regerada, o consumo antigo tem de sumir junto — um append
    deixaria o ledger somando duas capturas e o total nao denunciaria nada.
    """
    import pyarrow as pa

    from .config import from_env
    from .query import connect_lakehouse

    config = config or from_env()
    conexao = connect_lakehouse(config)
    try:
        # `.arrow()` devolve um RecordBatchReader no duckdb 1.5, e nao uma Table.
        # `fetch_arrow_table()` materializa — o agregado tem dezenas de milhares de
        # linhas, nao milhoes, entao nao ha o que streamar.
        tabela_arrow = conexao.execute(CONSUMO_SQL).fetch_arrow_table()
    except Exception as exc:
        raise StockLedgerError(
            f"nao foi possivel ler o consumo do Silver: {exc}\n"
            "Rode `make silver` antes — este verbo le silver_order_line e silver_order."
        ) from exc

    esquema = consumption_schema()
    colunas = [tabela_arrow.column(campo.name).cast(campo.type)
               for campo in esquema if campo.name != "written_by"]
    colunas.append(pa.array(["platform"] * tabela_arrow.num_rows, type=pa.string()))
    lote = pa.Table.from_arrays(colunas, schema=esquema)

    cat = cat or catalog(config)
    cat.create_namespace_if_not_exists(NAMESPACE)
    if not cat.table_exists(CONSUMPTION_TABLE):
        cat.create_table(CONSUMPTION_TABLE, schema=esquema)
    tabela = cat.load_table(CONSUMPTION_TABLE)
    tabela.overwrite(lote)

    dias = tabela_arrow.column("stock_date").to_pylist()
    return {
        "linhas": lote.num_rows,
        "series": len({(w, p) for w, p in zip(tabela_arrow.column("wh").to_pylist(),
                                              tabela_arrow.column("source_product_id").to_pylist())}),
        "unidades": sum(tabela_arrow.column("units_consumed").to_pylist()),
        "de": min(dias).isoformat() if dias else None,
        "ate": max(dias).isoformat() if dias else None,
        "metadata_location": cat.load_table(CONSUMPTION_TABLE).metadata_location,
    }


def metadata_location(table: str = LEDGER_TABLE, cat=None) -> str | None:
    """Onde esta o metadado CORRENTE da tabela, segundo o catalogo — ou None se nao existir.

    None nao e erro: o Spark e OPCIONAL neste projeto. Um repositorio recem-clonado, ou uma
    maquina onde ninguem rodou `make stock-ledger`, tem de construir o Silver em paz. Quem
    decide o que fazer com o None e o `silver_gate`, num lugar so.

    Nunca varre o storage atras do metadado mais novo: o DuckDB recusa fazer isso e a recusa
    esta certa — o arquivo mais recente pode ser um commit que nao aconteceu.
    """
    try:
        cat = cat or catalog()
        if not cat.table_exists(table):
            return None
        return cat.load_table(table).metadata_location
    except Exception:
        return None
