# Design Decisions Log

This document records key architectural decisions made during the project,
including the context, options considered, and reasoning behind each choice.

---

## Decision 1: Databricks Premium over Community Edition

**Date:** May 2026
**Status:** Decided

### Context
Needed a cloud data platform to host the Lakehouse. Databricks offers both
a free Community Edition and a paid Premium tier.

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Community Edition | Free | No Auto Loader, no Lakeflow Connect, no Databricks Apps |
| Premium tier | Full feature set, production-grade | Costs ~$30-80/month |

### Decision
Chose Premium tier. Auto Loader and Lakeflow Connect are core ingestion
features that appear in real job descriptions. Building without them would
undermine the project's goal of demonstrating production-grade skills.

### What I Would Do Differently
Set up cost alerts from Day 1 to avoid surprise bills.

---

## Decision 2: PySpark Notebooks for Bronze Instead of Delta Live Tables

**Date:** May 2026
**Status:** Decided

### Context
Needed a framework to ingest raw files into the Bronze layer.
Two main options: PySpark notebooks or Delta Live Tables (DLT).

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Delta Live Tables | Declarative, built-in expectations, auto-lineage | Extra cost per DBU, more complex to debug |
| PySpark notebooks | Full control, easier to debug, free | More boilerplate code |

### Decision
Chose PySpark notebooks. The engineering concepts are identical —
Auto Loader, file hash deduplication, schema enforcement, dead letter queue —
just implemented explicitly. This gives more control and is easier to explain
in interviews. DLT would be the right choice in a larger team setting.

---

## Decision 3: dbt for Silver and Gold Instead of Pure Databricks SQL

**Date:** May 2026
**Status:** Decided

### Context
Needed a transformation framework for Silver and Gold layers.

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Pure Databricks SQL notebooks | Databricks-native | No testing, no docs, no lineage graph, not portable |
| dbt Core + dbt-databricks | Industry standard, testing, docs, CI/CD, portable | Extra setup |

### Decision
Chose dbt. It is the #1 skill on Analytics Engineer job descriptions.
It generates a lineage graph and documentation site automatically.
It enables slim CI with state:modified+. Skills transfer to any platform.

---