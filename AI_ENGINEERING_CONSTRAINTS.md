# Engineering constraints for AI/agents

**This document is not mine.** The body below is the technical counter-position text written
by **welton.ferreira** and delivered to the agent on **2026-09-01**, transcribed **without
alteration** from the message that contained it. It was not version-controlled, and a
contract that only exists in a conversation doesn't govern the repository: a future agent
clones the project and never sees it. That's why it's here — and authorship is the reason the
text was **not** reformatted, summarized, or "improved."

**Translation note (2026-09-04).** The paragraph above, and the body that follows, were
originally written in Portuguese. At the author's own request, the whole project — this
document included — is being translated to English. This is a faithful, meaning-preserving
translation, not a rewrite: nothing was added, removed, softened, or reinterpreted. The
original Portuguese text is preserved byte-for-byte in the git history of this file. If any
line here reads as ambiguous against the author's intent, the original Portuguese commit is
the tiebreaker, not this translation.

**Precedence.** This document overrides any agent's preference. The decision hierarchy is in
its own section 22, and AI preference is the **last** criterion.

**Where it was exercised.** All of Phase 7 was conducted against this text, and the record of
each decision — with the section that caused it — is in [DECISIONS.md](DECISIONS.md). The
*change request* template required by STATUS closes that file. Scope that doesn't make the
cut lives in [BACKLOG.md](BACKLOG.md).

> **Check this transcript.** It was recovered from the session log, not from a file you had
> committed. If any paragraph isn't your text, your text is what governs.

---

AI_ENGINEERING_CONSTRAINTS.md

Technical counter-position document — Phase 7 and freeze

Purpose: establish limits for any AI/agent that continues working on the project.

This document overrides as the set of engineering constraints for the final stage. The AI
must preserve decisions already demonstrated and must not introduce architectural changes
merely out of preference, technological novelty, or generic "best practice."

1. Main rule

Before modifying architecture, technology, data contract, or operational assumption, the AI
must answer:

What concrete problem exists?

What evidence demonstrates that it exists?

Why isn't the current solution sufficient?

What is the cost of the change?

What test will prove the change improved the system?

What will be lost or altered by the change?

If these questions cannot be answered, do not make the change.

2. The project must not become a technology collection

Technology is not justification.

Do not add:

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

any new database, framework, or service

just to increase the tool count.

Every technology needs an explicit architectural responsibility and evidence of necessity.

3. Rule for Spark

Spark must not be defended by the current data volume.

The use case is the stock ledger / replenishment, where future state depends on prior state:

consumption -> balance -> reorder -> receipt -> future balance

This chain is the technical argument for distributed/stateful processing.

Spark must:

read/write the same Iceberg catalog used by the project;

produce verifiable data;

maintain provenance;

run under an explicit profile;

not make the project's default path depend on Spark.

Do not turn DuckDB into "wrong" just because Spark was introduced.

The spike's goal is to demonstrate interoperability and engine fit for the problem, not to
prove Spark is faster.

4. Rule for Iceberg

Iceberg is the project's table/concurrency mechanism.

Do not:

guess `metadata_location`;

manually reconstruct metadata;

blind-overwrite after a conflict;

retry in a way that could replace newer state with older state.

On optimistic conflict:

detect the conflict;

reload state;

reapply the operation;

respect sequence monotonicity;

retry.

The rule is:

older seq MUST NOT overwrite newer seq.

5. Rule for Kafka

Kafka is transport, not the canonical source.

Adopted semantics:

publish -> broker ack -> mark outbox

The possibility of duplication in the window between ACK and marking the outbox is accepted.

Therefore:

do not promise end-to-end exactly-once;

keep deduplication in the consumer;

respect ordering by order_id;

seq <= last -> discard;

seq == last + 1 -> apply;

seq > last + 1 -> detect a gap and stop processing that sequence.

Do not change the semantics to "exactly once" without a complete technical demonstration.

6. Rule for Outbox

Business state and event must have transactional atomicity.

The operation must keep:

business state + outbox event

in the same transaction.

Rollback must undo both.

Routing columns must stay consistent with the payload.

last_sequence_no must preserve ordering.

7. Rule for sources

Frozen sources are contracts.

Do not silently modify:

format;

semantics;

identifiers;

partitions;

checksums;

RAW content.

RAW must remain a faithful representation of the origin.

If an analytical interpretation is needed, it must happen downstream.

8. Rule for INE / Callejero

Do not interpret Callejero as a simple "one row per street" table.

TRAM can have multiple legitimate rows for the same street because it represents
segments/numbering ranges and can differentiate:

census section;

numbering type;

start/end number;

postal code;

segment.

Therefore:

street-level != tramo-level

Do not deduplicate these rows without preserving the granularity.

The warehouse/service-area relationship belongs to the business simulation and must not be
artificially assigned by the Callejero source.

9. Rule for Customer

Customer is synthetic.

Its geography must be anchored to real Lakehouse data, but the association with a warehouse
represents a simulation assumption.

Do not present synthetic customers as real data.

Keep separation between:

source key;

business identity;

real geographic reference;

synthetic entity.

10. Rule for Orders

Orders are synthetic data, but must have:

temporal coherence;

coherence between header and lines;

events;

states;

quantities;

prices;

relationships to products/customers.

Do not change field semantics just to produce "prettier" numbers.

When there's divergence between folds, fix the semantics and reconcile.

11. Demand / MAPA rule

MAPA is a calibration benchmark, not a literal copy of the e-commerce basket.

The logic must distinguish:

observed consumption;

consumption benchmark;

conversion to kg/L;

e-commerce behavior;

synthetic data.

Do not use price as a hidden mechanism to produce demand share.

Observed price must be an economic consequence of the chosen product, not a hidden selection
mechanism.

For products sold by weight/unit, preserve RAW semantics and derive appropriate analytical
fields, such as:

purchasable_unit_price;

price_basis;

net_content_kg_l.

The benchmark must calibrate volume primarily; value is a consequence of observed price.

When the benchmark lacks sufficient granularity, explicitly declare the heuristic used.

12. Observed versus synthetic data rule

Never hide data origin.

The documentation must make clear:

Real / observed

Mercadona catalog;

INE;

Callejero;

MAPA;

other official sources actually used.

Synthetic

Customers;

Orders;

Stock;

Delivery;

simulated events;

business behavior not provided by the sources.

The project is an engineering platform based on real data + controlled synthetic universes.

That is a feature, not a deficiency to hide.

13. Reproducibility rule

External sources may not be reproducible.

Therefore:

external source -> frozen capture -> deterministic downstream

The final stage must record evidence of:

partition;

SHA-256;

count;

time range;

capture state.

Downstream must be deterministic when given the same RAW.

Do not promise reproduction of the external source when it depends on a live API, manual
download, or a change by the provider.

14. Validation rule

A test is not decoration.

Any relevant change must have evidence.

Tests must keep existing for:

schema;

referential integrity;

temporality;

atomicity;

ordering;

deduplication;

gaps;

concurrency;

stock invariants;

provenance;

freeze;

reconciliation between folds.

Negative tests matter.

An implementation that "always passes" proves no quality.

15. Documentation rule

The final documentation must be short and operational.

Expected structure:

README — overview and how to run;

ARCHITECTURE — architecture and flows;

DECISIONS — decisions and trade-offs;

evidence/tests — executable proof.

Do not duplicate the same explanation across five documents.

Do not write promotional documentation.

Document:

decision -> reason -> evidence -> trade-off

16. Rule against overengineering

Before creating a component, ask:

"What real project problem does this component solve?"

If the answer is only:

"it's used in the industry";

"it looks more professional";

"it's a best practice";

"companies use it";

"might be useful someday";

"looks good on a résumé";

the implementation must be rejected.

17. Observability

Observability is desirable, but must not delay the project's functional closure.

If implemented:

application -> OpenTelemetry -> Collector/Alloy -> backend

Grafana/Prometheus must have a clear operational purpose.

Do not add dashboards just to generate screenshots.

First prove useful signals exist:

latency;

error;

throughput;

failures;

processing;

pipeline state.

18. Snowflake

Snowflake must receive the appropriate analytical/serving layer.

Do not copy RAW into Snowflake indiscriminately just because the project has Snowflake.

The conceptual architecture is:

RAW/Silver -> curated/serving -> Snowflake

The warehouse must answer analytical questions.

Do not turn Snowflake into an arbitrary second copy of the Data Lake.

19. BI

The dashboard must demonstrate data consumption.

Priority:

model quality;

correct metrics;

traceability;

visual clarity.

The BI tool must not drive upstream architecture.

Power BI/Tableau/Streamlit are consumption layers.

20. Closing criterion

Once Phase 7 meets its gates, the project must be FROZEN.

Do not open a new phase simply because another interesting technology exists.

New ideas must go to:

BACKLOG / FUTURE WORK

and not into the main codebase.

21. Mandatory questions before any change

The AI must answer internally:

Need

What problem am I fixing?

Evidence

What test/log/metric demonstrates the problem?

Architecture

Which component should be responsible?

Compatibility

What existing contracts will be affected?

Regression

What tests might break?

Semantics

Am I changing the meaning of any data?

Provenance

Will we still know where the data came from?

Reproducibility

Will the same input keep producing the same result?

Cost

Is the added complexity justifiable?

Closure

Is this necessary for the project's goal, or is it just a future improvement?

22. Decision hierarchy

When there's conflict between generic "best practice" and the project's evidence:

data contract;

invariants;

tests;

experimental evidence;

recorded architectural decision;

simplicity;

AI preference.

AI preference is the last criterion.

23. Explicit prohibition

The AI must NOT:

rewrite the entire architecture;

swap technologies out of preference;

add services without need;

remove components without analyzing their responsibilities;

silently change field semantics;

deduplicate legitimate data by appearance;

claim performance without a benchmark;

claim production-grade without operational evidence;

treat synthetic data as real;

turn a benchmark into ground truth;

implement "future proofing" without a requirement;

create new features during the freeze.

24. Final objective

This project's goal is not to have the largest possible number of technologies.

The goal is to demonstrate that the engineer can:

model a problem;

build pipelines;

preserve contracts;

handle failures;

guarantee idempotency;

work with events;

handle concurrency;

reconcile different data paths;

explain trade-offs;

measure before asserting;

distinguish real data from synthetic data;

choose tools according to the problem.

A smaller architecture that can prove its properties is superior to a larger architecture
that only looks sophisticated.

STATUS

Phase 7: technical closure

After the final gates:

FREEZE

Any later change must be treated as:

a change request

and require justification, impact, tests, and a recorded decision.
