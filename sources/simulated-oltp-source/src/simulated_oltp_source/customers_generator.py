"""Amostragem dos clientes sinteticos. Logica pura: entra referencia, sai lista de dicts.

REGRA DE OURO: zero aleatoriedade geografica. O cliente e inventado; o lugar onde ele
mora nao. Municipio, via, CEP e faixa de numeracao vem sempre de uma linha real do
Callejero; nada aqui constroi um endereco a partir de partes soltas.

CADEIA DE AMOSTRAGEM (nesta ordem, sempre):

    warehouse -> municipio ponderado pela populacao municipal observada
              -> tramo UNIFORME entre os candidatos validos do municipio
              -> numero da casa dentro da faixa real, respeitando a paridade
              -> sexo pela proporcao municipal observada
              -> idade pela distribuicao PROVINCIAL (proxy)
              -> nome de lista curada embutida

A escolha do tramo e UNIFORME de proposito, e isso NAO e densidade populacional: nao
existe populacao por rua ou por tramo em nenhuma fonte que esta plataforma ingere.
Ponderar tramos por qualquer coisa aqui seria inventar uma distribuicao que ninguem
mediu. O peso populacional entra uma unica vez, na escolha do municipio.

REPRODUTIBILIDADE: uma unica instancia `random.Random(seed)` por execucao, consumida
sempre na mesma ordem. As listas percorridas vem do JSON de referencia, cuja ordem e
fixada por `ORDER BY` no export — nunca se itera `set` nem `dict` reconstruido, cuja
ordem dependeria de PYTHONHASHSEED e quebraria a mesma-seed-mesma-saida.
"""

from __future__ import annotations

import itertools
import random

# Vocabulario de sexo: os rotulos EXATOS da tabela 29005 do INE, de onde vem a proporcao.
# O campo se chama `sex_label` pelo mesmo motivo que a coluna homonima do Silver: o valor
# e o rotulo da fonte, nao uma categoria que esta Source inventou. Assim o join de volta
# com silver_ine_population_by_municipality e direto e sem traducao.
SEX_MALE = "Hombres"
SEX_FEMALE = "Mujeres"

# Numeracao do Callejero (campo TINUM), verbatim: "0" = nao existe numeracao real,
# "1" = impares, "2" = pares. Carregado ate o cliente para que a regra de numeracao seja
# auditavel olhando so o customers.json.
NUMBERING_NONE = "0"
NUMBERING_ODD = "1"
NUMBERING_EVEN = "2"

# Dados literais, nao dependencia: nomes proprios e sobrenomes espanhois de uso comum.
# Servem so para dar forma humana ao registro — nao ha nenhuma fonte de nomes reais no
# Lakehouse, e inventar uma seria fingir precisao que nao existe.
FIRST_NAMES_MALE = (
    "Antonio", "Manuel", "José", "Francisco", "David", "Juan", "Javier", "Daniel",
    "Carlos", "Miguel", "Alejandro", "Rafael", "Pablo", "Jesús", "Ángel", "Sergio",
    "Fernando", "Pedro", "Jorge", "Alberto", "Luis", "Álvaro", "Adrián", "Diego",
    "Raúl", "Iván", "Rubén", "Óscar", "Enrique", "Andrés", "Marcos", "Víctor",
)
FIRST_NAMES_FEMALE = (
    "María", "Carmen", "Ana", "Isabel", "Laura", "Cristina", "Marta", "Lucía",
    "Elena", "Sara", "Paula", "Raquel", "Silvia", "Beatriz", "Rosa", "Patricia",
    "Nuria", "Andrea", "Alba", "Julia", "Irene", "Claudia", "Natalia", "Sofía",
    "Eva", "Pilar", "Teresa", "Susana", "Mónica", "Alicia", "Noelia", "Sandra",
)
SURNAMES = (
    "García", "Rodríguez", "González", "Fernández", "López", "Martínez", "Sánchez",
    "Pérez", "Gómez", "Martín", "Jiménez", "Ruiz", "Hernández", "Díaz", "Moreno",
    "Muñoz", "Álvarez", "Romero", "Alonso", "Gutiérrez", "Navarro", "Torres",
    "Domínguez", "Vázquez", "Ramos", "Gil", "Ramírez", "Serrano", "Blanco", "Molina",
    "Morales", "Suárez", "Ortega", "Delgado", "Castro", "Ortiz", "Rubio", "Marín",
    "Sanz", "Núñez", "Iglesias", "Medina", "Garrido", "Cortés", "Castillo", "Santos",
    "Lozano", "Guerrero", "Cano", "Prieto", "Méndez", "Cruz", "Calvo", "Gallego",
)


class GenerationError(Exception):
    """A referencia nao sustenta a geracao pedida."""


def pick_house_number(rng: random.Random, numbering_type, number_from, number_to):
    """Numero de casa dentro da faixa real do tramo, ou None quando nao existe.

    Tres motivos independentes para nao haver numero, todos medidos no Callejero e todos
    resolvidos com None em vez de um numero inventado:

      1. `numbering_type == "0"` (9.024 dos 216.594 tramos no escopo): a fonte declara que
         a via nao tem esquema de numeracao. Sortear aqui fabricaria geografia — a mesma
         proibicao do CEP, um nivel abaixo.
      2. Faixa degenerada `0000..0000` com numeracao declarada (14 tramos com
         `numbering_type == "2"`): sortear em [0,0] daria numero de casa 0, que nao existe.
         O filtro por numbering_type sozinho NAO pega este caso.
      3. Faixa sem nenhum numero da paridade declarada (ex.: pares entre 3 e 3).

    Fora esses casos, o numero respeita a paridade: impar para "1", par para "2".
    """
    if numbering_type == NUMBERING_NONE:
        return None
    if numbering_type not in (NUMBERING_ODD, NUMBERING_EVEN):
        return None
    if not isinstance(number_from, int) or not isinstance(number_to, int):
        return None

    low, high = min(number_from, number_to), max(number_from, number_to)
    low = max(low, 1)  # 0 nunca e um numero de casa real
    if high < low:
        return None

    parity = 1 if numbering_type == NUMBERING_ODD else 0
    first = low if low % 2 == parity else low + 1
    if first > high:
        return None
    return first + 2 * rng.randrange((high - first) // 2 + 1)


def _cumulative(weights: list[float]) -> list[float]:
    return list(itertools.accumulate(float(w) for w in weights))


def generate(reference, wh: str, count: int, seed: int, ingestion_date: str) -> list[dict]:
    """Gera `count` clientes do warehouse `wh`. Mesma (referencia, wh, count, seed,
    ingestion_date) produz exatamente a mesma lista, byte a byte."""
    if count < 1:
        raise GenerationError(f"count deve ser >= 1, recebido {count}")

    rng = random.Random(seed)
    municipalities = reference.municipalities(wh)

    # Pesos cumulativos calculados uma vez. `random.choices` faria o mesmo a cada chamada:
    # o resultado e identico, o custo nao.
    municipality_cum = _cumulative([m["proportion_within_wh"] for m in municipalities])

    # Cache por chave ja vista. So leitura: nunca se ITERA este dict, so se consulta,
    # entao a ordem de insercao nao influencia nenhuma amostragem.
    candidates_cache: dict[tuple, list[int]] = {}
    ages_cache: dict[str, tuple[list[int], list[float]]] = {}

    # birth_year deriva da data da PARTICAO, nunca do relogio: senao a mesma seed daria
    # saidas diferentes conforme o dia em que a extracao rodasse.
    reference_year = int(ingestion_date[:4])

    customers: list[dict] = []
    for index in range(count):
        municipality = rng.choices(municipalities, cum_weights=municipality_cum, k=1)[0]
        province_code = municipality["province_code"]
        municipality_code = municipality["municipality_code"]

        key = (wh, province_code, municipality_code)
        if key not in candidates_cache:
            candidates_cache[key] = reference.candidates_of(*key)
        candidate_index = rng.choice(candidates_cache[key])
        candidate = reference.candidate(candidate_index)

        house_number = pick_house_number(
            rng,
            candidate["numbering_type"],
            candidate.get("number_from"),
            candidate.get("number_to"),
        )

        male = rng.random() < float(municipality["sex_hombres_proportion"])
        sex_label = SEX_MALE if male else SEX_FEMALE

        if province_code not in ages_cache:
            ages, proportions = reference.ages_of(province_code)
            ages_cache[province_code] = (ages, _cumulative(proportions))
        ages, age_cum = ages_cache[province_code]
        age = rng.choices(ages, cum_weights=age_cum, k=1)[0]

        given = FIRST_NAMES_MALE if male else FIRST_NAMES_FEMALE
        first_name = rng.choice(given)
        # Espanha usa dois sobrenomes: paterno e materno.
        last_name = f"{rng.choice(SURNAMES)} {rng.choice(SURNAMES)}"

        customers.append(
            {
                "customer_id": f"cust_{wh}_{index:06d}",
                "wh": wh,
                "province_code": province_code,
                "province_name": municipality["province_name"],
                "municipality_code": municipality_code,
                "municipality_name": municipality["municipality_name"],
                "candidate_index": candidate_index,
                "street_name": candidate["street_name"],
                "postal_code": candidate["postal_code"],
                "numbering_type": candidate["numbering_type"],
                # Chave SEMPRE presente, mesmo nula: `schema.fingerprint` e a uniao das
                # chaves observadas, entao um campo opcional faria a impressao digital da
                # particao depender da seed.
                "house_number": house_number,
                "first_name": first_name,
                "last_name": last_name,
                "sex_label": sex_label,
                "birth_year": reference_year - age,
            }
        )
    return customers
