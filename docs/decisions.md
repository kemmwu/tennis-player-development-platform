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

## Decision 4: Claude API over Open-Source LLMs

**Date:** May 2026
**Status:** Decided

### Context
Needed an LLM to extract structured data from SwingVision screenshots
and coaching notes. Options ranged from open-source models to
commercial APIs.

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Open-source (Llama, Mistral) | Free, runs locally | Lower accuracy on domain-specific vision tasks, complex setup |
| OpenAI GPT-4o | Strong vision capabilities | Higher cost per token |
| Claude API (claude-sonnet) | Best accuracy on structured extraction, multimodal, affordable | API cost per call |

### Decision
Chose Claude API. In head-to-head testing on 20 labeled screenshots,
Claude achieved higher field-level accuracy on tennis-specific statistics.
The structured JSON output with confidence scoring was also more reliable.
At the project's scale (~200 screenshots/month), cost is under $5/month.

### What I Would Do Differently
Run a more formal benchmark across all three models before committing,
rather than relying on informal testing.

---

## Decision 5: Lakeflow Connect + Kafka for Intake Events

**Date:** May 2026
**Status:** Decided

### Context
Student intake form submissions needed to reach the Bronze layer.
Options ranged from simple file drops to real streaming infrastructure.

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Manual CSV export | Simplest possible | Not event-driven, requires manual steps |
| Webhook → Volume file drop (Make.com) | Simple, free | Simulated streaming, not production pattern |
| Lakeflow Connect + Kafka | Real production pattern, demonstrates streaming skills | More complex setup, paid feature |

### Decision
Chose Lakeflow Connect + Kafka. Even at low event volume (a few
intake forms per week), the architecture is fully production-grade.
Every real company uses event-driven ingestion for form submissions.
Demonstrating this pattern in a portfolio project immediately signals
you understand production streaming, not just batch processing.

### What I Would Do Differently
Start with the simulated webhook approach first to validate the
schema, then migrate to Kafka once the data model is stable.

---

## Decision 6: SCD Type 2 for Student Profiles

**Date:** May 2026
**Status:** Decided

### Context
Student profiles change over time — UTR rating improves, age group
changes, training frequency changes. Needed to decide how to handle
these changes in the data model.

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Overwrite (SCD Type 1) | Simple, always current | Lose history, can't analyze how profile changes correlated with performance |
| SCD Type 2 | Full history retained | More complex query logic |
| Snapshot table (dbt snapshots) | dbt-native, easy to implement | Slightly more storage |

### Decision
Chose SCD Type 2 implemented via dbt snapshots. As a coach, tracking
how a student's profile changed over time is directly useful — for
example, understanding whether performance improved after UTR rating
crossed a certain threshold. The historical data has real analytical
value, not just engineering completeness.

### What I Would Do Differently
Document the SCD Type 2 query patterns for downstream consumers earlier,
as joining against a Type 2 table confuses analysts who are not familiar
with the pattern.

---

## Decision 7: 7-Day Window for Notes-to-Match Linkage

**Date:** May 2026
**Status:** Decided

### Context
The `int_notes_with_match_linkage` model needed a time window to JOIN
coaching notes to subsequent match performance. The window length is
a domain decision, not a technical one.

### Options Considered
| Window | Reasoning |
|---|---|
| 3 days | Too short — technique changes rarely show in matches within 3 days |
| 7 days | One training week — coaching feedback typically surfaces in the next match |
| 14 days | More conservative — accounts for tournament schedules |
| 30 days | Too broad — too many confounding variables |

### Decision
Chose 7 days based on coaching experience. In practice, a student
who receives technique feedback on Monday will typically play their
next competitive match within the same week. A 7-day window captures
this relationship without introducing noise from unrelated sessions.

### What I Would Do Differently
Treat the window length as a configurable dbt variable rather than
hardcoding it, so it can be adjusted based on empirical analysis
once enough data is collected.

---

## Decision 8: Synthetic Data for Historical Volume

**Date:** May 2026
**Status:** Decided

### Context
The project needed enough historical data to demonstrate window
functions, partitioning, Z-ordering, and trend analysis. Real coaching
data covered only a few months.

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Use only real data | Fully authentic | Too small for meaningful performance demos |
| Purchase third-party tennis data | Realistic | Expensive, licensing issues |
| Generate synthetic data with Faker | Full control, free, scalable | Not real data |

### Decision
Chose synthetic data generation with Python + Faker. The synthetic
data is clearly documented as such in the README — this is an
engineering integrity choice, not a deception. The data follows
realistic distributions (win rates, UTR ratings, match frequencies)
based on coaching knowledge. This allows demos of 100k+ row fact
tables and meaningful trend analysis.

### What I Would Do Differently
Build the synthetic data generator as a proper configurable script
from Day 1, rather than treating it as a quick utility. A well-designed
generator is easier to extend and easier to explain in interviews.

---

## Known Limitations

This section honestly documents what was not built or what could be
improved with more time.

- **Data volume:** Fact tables contain synthetic data at ~100k rows.
  Real production systems handle billions. Z-ordering and partitioning
  strategies are demonstrated but not stress-tested at true scale.

- **LLM accuracy:** Vision extraction achieves ~94% field-level accuracy
  on the eval set. The remaining 6% still requires human review.
  A production system would need higher accuracy before reducing HITL.

- **No multi-tenancy:** The platform supports one coach. Supporting
  multiple coaches would require row-level security in Unity Catalog
  and tenant isolation in the Streamlit apps.

- **Entity resolution:** Player name aliases are resolved via a manual
  mapping table. A production system would use fuzzy matching or
  LLM-based resolution to handle this automatically.

- **Streaming volume:** Lakeflow Connect + Kafka is configured for
  low-volume intake events. True high-throughput streaming would
  require tuned Spark Structured Streaming configurations.

---

## Decision 9: Screenshot Filename Convention as Session Identifier

**Date:** May 2026
**Status:** Decided

### Context
SwingVision generates multiple screenshots per session — a player's match
or training stats are spread across several pages (summary, serve stats,
rally stats, notes). These screenshots are uploaded separately to the
Volume, so the pipeline needs a way to:
1. Know which screenshots belong to the same session
2. Distinguish between a morning session and an evening session
   for the same player on the same day
3. Link coaching notes (also captured as screenshots) to their
   parent session without a separate upload step

### Options Considered
| Option | Pros | Cons |
|---|---|---|
| Match by player + date only | Simple | Breaks if two sessions same day |
| Manual mapping CSV | Explicit, zero ambiguity | Requires maintenance every upload |
| Separate notes folder | Clean separation | Notes and stats become two ingestion pipelines to join |
| Filename convention with time identifier | Self-describing, deterministic, zero maintenance | Requires discipline at upload time |

### Decision
Enforced a strict filename convention:

YYYYMMDDHHNN_firstname_match_N.png
YYYYMMDDHHNN_firstname_training_N.png

The `YYYYMMDDHHNN` prefix (date + start time in 24h format) acts as a
unique session identifier. All screenshots sharing the same prefix belong
to the same session. The trailing `_N` identifies the page number within
that session.

Examples:
- `202503151400_alex_match_1.png` — Alex's 2pm match, page 1
- `202503151400_alex_match_2.png` — same match, page 2
- `202503151400_alex_match_3.png` — same match, page 3 (notes)
- `202503151900_alex_training_1.png` — Alex's 7pm training, different session

The ingestion notebook parses the filename to extract `player_name`,
`session_date`, `session_time`, and `session_type` automatically.
Claude Vision then identifies the screenshot type (stats page vs notes
page) and extracts the appropriate fields from each.

This design means coaching notes require no separate upload — they are
captured as the last screenshot in a session group and identified
automatically by Claude.

### Why This Works for This Domain
As a coach uploading screenshots manually after each session, the naming
convention costs nothing extra — I am already naming files when saving
them. The time identifier also matches how I naturally think about
sessions: "Alex's 2pm match" is unambiguous even if he trains again at 7pm.

### What I Would Do Differently
Build a small mobile shortcut (iOS Shortcut or similar) that
auto-renames screenshots to this convention at capture time, eliminating
any chance of naming errors before upload.

**Why store raw counts instead of percentages:**
SwingVision shows serve stats as "18/36 (50%)". We store the raw counts
(18 and 36) rather than the percentage. The Silver layer calculates
`first_serves_in_pct = first_serves_in / first_serves_total`.
This avoids rounding loss and gives downstream models more flexibility.


---

## Decision 10: 7-Day Window for Note-to-Match Linkage

**Date:** May 2026
**Status:** Decided

### Context
Coaching notes are written after training sessions. To measure whether coaching
observations predict match outcomes, we need to link notes to subsequent matches.
The question is: what time window makes sense?

### Options Considered
| Window | Pros | Cons |
|---|---|---|
| 3 days | Very tight causal link | Misses most matches — students don't compete that frequently |
| 7 days | Matches weekly competition cycle | Occasional false links for bi-weekly competitors |
| 14 days | Catches more matches | Too loose — many unrelated events in two weeks |
| Session-level tagging | Perfect precision | Requires manual tagging by coach — not scalable |

### Decision
7-day window. Most junior and recreational players compete once per week.
A coaching note written after Monday training is preparation for the weekend
match. 7 days captures this relationship without being so loose that it links
unrelated sessions.

### What I Would Do Differently
Add a `target_match_date` field to the coaching note at capture time — letting
the coach explicitly tag which match a note is preparing for. This would make
the linkage exact rather than time-window-based.