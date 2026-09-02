# Restrições de engenharia para IA/agentes

**Este documento não é meu.** O corpo abaixo é o texto de contraposição técnica escrito por
**welton.ferreira** e entregue ao agente em **2026-09-01**, transcrito **sem alteração** da
mensagem que o continha. Ele não estava versionado, e um contrato que só existe numa conversa
não governa o repositório: um agente futuro clona o projeto e não o encontra. Por isso ele
está aqui — e a autoria é a razão pela qual o texto **não** foi reformatado, resumido nem
"melhorado".

**Precedência.** Este documento prevalece sobre a preferência de qualquer agente. A hierarquia
de decisão está na sua própria seção 22, e a preferência da IA é o **último** critério.

**Onde ele foi exercido.** A Fase 7 inteira foi conduzida contra este texto, e o registro de
cada decisão — com a seção que a causou — está em [DECISIONS.md](DECISIONS.md). O template de
*change request* exigido pelo STATUS fecha aquele arquivo. Escopo que não entra vive em
[BACKLOG.md](BACKLOG.md).

> **Confira esta transcrição.** Ela foi recuperada do log da sessão, não de um arquivo que
> você tenha commitado. Se algum parágrafo não for o seu texto, o seu texto é o que vale.

---

AI_ENGINEERING_CONSTRAINTS.md

Documento de contraposição técnica — Fase 7 e congelamento

Objetivo: estabelecer limites para qualquer IA/agente que continue trabalhando no projeto.

Este documento prevalece como conjunto de restrições de engenharia para a etapa final. A IA deve preservar as decisões já demonstradas e não introduzir mudanças arquiteturais apenas por preferência, novidade tecnológica ou "melhor prática" genérica.

1. Regra principal

Antes de modificar arquitetura, tecnologia, contrato de dados ou premissa operacional, a IA deve responder:

Qual problema concreto existe?

Qual evidência demonstra que ele existe?

Por que a solução atual não é suficiente?

Qual é o custo da mudança?

Qual teste provará que a mudança melhorou o sistema?

O que será perdido ou alterado com a mudança?

Se essas perguntas não puderem ser respondidas, não alterar.

2. O projeto não deve virar uma coleção de tecnologias

Tecnologia não é justificativa.

Não adicionar:

Spark;

Kafka;

Iceberg;

Airflow;

Snowflake;

dbt;

OpenTelemetry;

Grafana;

Prometheus;

Databricks;

Redis;

qualquer novo banco, framework ou serviço

apenas para aumentar a quantidade de ferramentas.

Cada tecnologia precisa possuir uma responsabilidade arquitetural explícita e uma evidência de necessidade.

3. Regra para o Spark

O Spark não deve ser defendido pelo volume atual dos dados.

O caso de uso é o stock ledger / replenishment, no qual o estado futuro depende do estado anterior:

consumption -> balance -> reorder -> receipt -> future balance

Esse encadeamento constitui o argumento técnico para processamento distribuído/stateful.

O Spark deve:

ler/escrever no mesmo catálogo Iceberg utilizado pelo projeto;

produzir dados verificáveis;

manter provenance;

funcionar sob perfil explícito;

não tornar o caminho padrão do projeto dependente de Spark.

Não transformar DuckDB em "errado" apenas porque Spark foi introduzido.

O objetivo do spike é demonstrar interoperabilidade e adequação do engine ao problema, não provar que Spark é mais rápido.

4. Regra para Iceberg

Iceberg é o mecanismo de tabela/concurrency do projeto.

Não fazer:

guessing de metadata_location;

reconstrução manual de metadata;

overwrite cego após conflito;

retry que possa substituir estado novo por estado antigo.

Em conflito otimista:

detectar conflito;

recarregar estado;

reaplicar a operação;

respeitar monotonicidade de sequência;

tentar novamente.

A regra é:

older seq MUST NOT overwrite newer seq.

5. Regra para Kafka

Kafka é transporte, não fonte canônica.

Semântica adotada:

publish -> broker ack -> mark outbox

A possibilidade de duplicação no intervalo entre ACK e marcação do outbox é aceita.

Portanto:

não prometer exactly-once end-to-end;

manter deduplicação no consumidor;

respeitar ordenação por order_id;

seq <= last -> descartar;

seq == last + 1 -> aplicar;

seq > last + 1 -> detectar gap e interromper processamento daquela sequência.

Não alterar a semântica para "exactly once" sem uma demonstração técnica completa.

6. Regra para Outbox

Estado de negócio e evento devem possuir atomicidade transacional.

A operação deve manter:

business state + outbox event

na mesma transação.

Rollback deve desfazer ambos.

Routing columns devem permanecer consistentes com o payload.

last_sequence_no deve preservar ordenação.

7. Regra para fontes

Fontes congeladas são contratos.

Não modificar silenciosamente:

formato;

semântica;

identificadores;

partições;

checksums;

conteúdo RAW.

RAW deve permanecer como representação fiel da origem.

Se uma interpretação analítica for necessária, ela deve acontecer downstream.

8. Regra para INE / Callejero

Não interpretar Callejero como uma tabela simples de "uma linha por rua".

TRAM pode possuir múltiplas linhas legítimas para a mesma rua porque representa trechos/faixas de numeração e pode diferenciar:

seção censitária;

tipo de numeração;

número inicial/final;

código postal;

trecho.

Portanto:

street-level != tramo-level

Não deduplicar essas linhas sem preservar a granularidade.

A relação warehouse/service-area pertence à simulação de negócio e não deve ser artificialmente atribuída pelo source Callejero.

9. Regra para Customer

Customer é sintético.

Sua geografia deve ser ancorada em dados reais do Lakehouse, mas a associação com warehouse representa uma premissa da simulação.

Não apresentar clientes sintéticos como dados reais.

Manter separação entre:

source key;

business identity;

referência geográfica real;

entidade sintética.

10. Regra para Orders

Orders são dados sintéticos, mas devem possuir:

coerência temporal;

coerência entre header e lines;

eventos;

estados;

quantidades;

preços;

relações com produtos/clientes.

Não alterar a semântica de campos apenas para produzir números "mais bonitos".

Quando houver divergência entre folds, corrigir a semântica e reconciliar.

11. Regra de demanda / MAPA

MAPA é benchmark de calibração, não uma cópia literal do carrinho de ecommerce.

A lógica deve distinguir:

consumo observado;

benchmark de consumo;

conversão para kg/L;

comportamento de ecommerce;

dados sintéticos.

Não utilizar preço como mecanismo oculto para produzir share de demanda.

O preço observado deve ser consequência econômica do produto escolhido, não mecanismo escondido de seleção.

Para produtos vendidos por peso/unidade, preservar a semântica RAW e derivar campos analíticos adequados, como:

purchasable_unit_price;

price_basis;

net_content_kg_l.

O benchmark deve calibrar principalmente volume; valor é consequência do preço observado.

Quando o benchmark não tiver granularidade suficiente, declarar explicitamente a heurística utilizada.

12. Regra de dados observados versus sintéticos

Nunca esconder a origem dos dados.

A documentação deve deixar claro:

Real / observado

Mercadona catalog;

INE;

Callejero;

MAPA;

demais fontes oficiais efetivamente utilizadas.

Sintético

Customers;

Orders;

Stock;

Delivery;

eventos simulados;

comportamento de negócio não fornecido pelas fontes.

O projeto é uma plataforma de engenharia baseada em dados reais + universos sintéticos controlados.

Isso é uma característica, não uma deficiência a ser escondida.

13. Regra de reprodutibilidade

As fontes externas podem não ser reproduzíveis.

Por isso:

external source -> frozen capture -> deterministic downstream

A etapa final deve registrar evidência de:

partição;

SHA-256;

contagem;

intervalo temporal;

estado da captura.

O downstream deve ser determinístico quando recebe a mesma RAW.

Não prometer reprodução da fonte externa quando ela depende de API viva, download manual ou alteração do fornecedor.

14. Regra de validação

Teste não é decoração.

Qualquer alteração relevante deve possuir evidência.

Devem continuar existindo testes para:

schema;

integridade referencial;

temporalidade;

atomicidade;

ordenação;

deduplicação;

gaps;

concorrência;

invariantes de estoque;

provenance;

congelamento;

reconciliação entre folds.

Testes negativos são importantes.

Uma implementação que "sempre passa" não prova qualidade.

15. Regra de documentação

A documentação final deve ser curta e operacional.

Estrutura esperada:

README — visão geral e execução;

ARCHITECTURE — arquitetura e fluxos;

DECISIONS — decisões e trade-offs;

evidências/testes — provas executáveis.

Não duplicar a mesma explicação em cinco documentos.

Não escrever documentação promocional.

Documentar:

decision -> reason -> evidence -> trade-off

16. Regra contra overengineering

Antes de criar um componente, perguntar:

"Que problema real do projeto esse componente resolve?"

Se a resposta for apenas:

"é usado no mercado";

"fica mais profissional";

"é uma best practice";

"empresas usam";

"pode ser útil no futuro";

"fica bom no currículo";

a implementação deve ser rejeitada.

17. Observabilidade

Observabilidade é desejável, mas não deve interromper o fechamento funcional do projeto.

Se implementada:

application -> OpenTelemetry -> Collector/Alloy -> backend

Grafana/Prometheus devem possuir propósito operacional claro.

Não adicionar dashboards apenas para gerar screenshots.

Primeiro provar que existem sinais úteis:

latência;

erro;

throughput;

falhas;

processamento;

estado dos pipelines.

18. Snowflake

Snowflake deve receber a camada analítica/serving apropriada.

Não copiar RAW indiscriminadamente para Snowflake apenas porque o projeto possui Snowflake.

A arquitetura conceitual é:

RAW/Silver -> curated/serving -> Snowflake

O warehouse deve responder perguntas analíticas.

Não transformar Snowflake em segunda cópia arbitrária do Data Lake.

19. BI

O dashboard deve demonstrar consumo do dado.

Prioridade:

qualidade do modelo;

métricas corretas;

rastreabilidade;

clareza visual.

Ferramenta de BI não deve determinar a arquitetura upstream.

Power BI/Tableau/Streamlit são camadas de consumo.

20. Critério de encerramento

Depois que a Fase 7 cumprir seus gates, o projeto deve ser CONGELADO.

Não abrir uma nova fase simplesmente porque existe outra tecnologia interessante.

Novas ideias devem ir para:

BACKLOG / FUTURE WORK

e não para o código principal.

21. Perguntas obrigatórias antes de qualquer mudança

A IA deve responder internamente:

Necessidade

Qual problema estou corrigindo?

Evidência

Qual teste/log/métrica demonstra o problema?

Arquitetura

Qual componente deve ser responsável?

Compatibilidade

Que contratos existentes serão afetados?

Regressão

Quais testes podem quebrar?

Semântica

Estou alterando o significado de algum dado?

Provenance

Continuaremos sabendo de onde veio o dado?

Reprodutibilidade

O mesmo input continuará produzindo o mesmo resultado?

Custo

A complexidade adicionada é justificável?

Encerramento

Isso é necessário para o objetivo do projeto ou é apenas uma melhoria futura?

22. Hierarquia de decisão

Quando houver conflito entre "melhor prática" genérica e evidência do projeto:

contrato de dados;

invariantes;

testes;

evidência experimental;

decisão arquitetural registrada;

simplicidade;

preferência da IA.

A preferência da IA é o último critério.

23. Proibição explícita

A IA NÃO deve:

reescrever arquitetura inteira;

trocar tecnologias por preferência;

adicionar serviços sem necessidade;

remover componentes sem analisar suas responsabilidades;

alterar semântica de campos silenciosamente;

deduplicar dados legítimos por aparência;

alegar performance sem benchmark;

alegar production-grade sem evidência operacional;

tratar dados sintéticos como reais;

transformar benchmark em ground truth;

implementar "future proofing" sem requisito;

criar novas features durante o freeze.

24. Objetivo final

O objetivo deste projeto não é possuir o maior número possível de tecnologias.

O objetivo é demonstrar que o engenheiro consegue:

modelar um problema;

construir pipelines;

preservar contratos;

lidar com falhas;

garantir idempotência;

trabalhar com eventos;

lidar com concorrência;

reconciliar diferentes caminhos de dados;

explicar trade-offs;

medir antes de afirmar;

distinguir dado real de dado sintético;

escolher ferramentas de acordo com o problema.

Uma arquitetura menor que consegue provar suas propriedades é superior a uma arquitetura maior que apenas parece sofisticada.

STATUS

Fase 7: encerramento técnico

Após os gates finais:

FREEZE

Qualquer alteração posterior deve ser tratada como:

change request

e exigir justificativa, impacto, testes e decisão registrada.