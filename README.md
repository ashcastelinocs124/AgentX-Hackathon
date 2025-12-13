# AgentX: Enterprise SQL Agent Evaluation Framework

> A next-generation evaluation framework for LLM-powered SQL and data-engineering agents that surpasses Spider 2.0 through enterprise-grade realism, structural anti-hallucination mechanisms, and first-class observability.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## 🎯 Motivation

Existing SQL benchmarks have critical limitations:

| Benchmark             | Limitation                                                                                                                           |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| **Spider 1.0 / BIRD** | Single SQL queries, small schemas (<50 columns), synthetic data, single dialect                                                      |
| **Spider 2.0**        | Better realism (632 tasks, 6 dialects, dbt) but only ~21% success rate for SOTA agents; limited observability into _why_ agents fail |

**AgentX** addresses these gaps with:

- **Structural hallucination prevention** via ORM-powered schema grounding
- **Multi-dimensional scoring** beyond binary pass/fail
- **Automated error taxonomy** for systematic failure analysis
- **Enterprise-scale schemas** (3,000+ columns, nested types, cross-database queries)

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           AGENTX FRAMEWORK                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                      TASK ORCHESTRATOR                               │    │
│  │  • Load task definitions • Initialize environments • Coordinate flow │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                 ORM INFRASTRUCTURE LAYER (SQLAlchemy)                │    │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐    │    │
│  │  │ DatabaseMgr   │  │ SchemaInspect │  │ FixtureLoader         │    │    │
│  │  │ • Engines     │  │ • Tables      │  │ • ORM bulk insert     │    │    │
│  │  │ • Dialects    │  │ • Columns     │  │ • Raw bulk fallback   │    │    │
│  │  │ • Pooling     │  │ • Nested types│  │ • FK integrity        │    │    │
│  │  └───────────────┘  └───────────────┘  └───────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│          ┌─────────────────────────┼─────────────────────────┐              │
│          ▼                         ▼                         ▼              │
│  ┌───────────────┐    ┌─────────────────────┐    ┌───────────────────┐      │
│  │ AGENT SANDBOX │    │ HALLUCINATION       │    │ RESULT COMPARATOR │      │
│  │ • Tool APIs   │    │ DETECTOR            │    │ • pd.read_sql()   │      │
│  │ • GetSchema   │◄───│ • Column validator  │    │ • Multi-strategy  │      │
│  │ • SampleRows  │    │ • Table validator   │    │   comparison      │      │
│  │ • ValidateSQL │    │ • JOIN path verify  │    │                   │      │
│  └───────────────┘    └─────────────────────┘    └───────────────────┘      │
│          │                         ▲                         ▲              │
│          ▼                         │                         │              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                   RAW SQL EVALUATION LAYER                           │    │
│  │  • connection.execute(text(agent_sql)) — NO ORM ABSTRACTION          │    │
│  │  • Query plan capture • Execution timing • Error classification      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  EVALUATION SCORER              │  LOGGING & ERROR TAXONOMY          │    │
│  │  • Correctness • Hallucination  │  • Structured traces (JSONL)       │    │
│  │  • Efficiency • Grounding       │  • Auto-classification             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔑 Key Design Principles

### 1. ORM for Infrastructure, Raw SQL for Evaluation

```python
# ✅ ORM handles infrastructure
engine = create_engine("snowflake://user:pass@account/db")
inspector = inspect(engine)
columns = inspector.get_columns("orders")  # Schema introspection

# ✅ Raw SQL for agent evaluation (no abstraction interference)
with engine.connect() as conn:
    result = conn.execute(text(agent_generated_sql))  # Verbatim execution
```

### 2. Structural Hallucination Prevention

```python
class HallucinationDetector:
    def validate(self, sql: str, schema: SchemaSnapshot) -> ValidationResult:
        # Parse SQL AST → extract referenced identifiers
        # Compare against introspected schema
        # Flag phantom columns/tables/functions BEFORE execution
```

### 3. Multi-Dimensional Scoring

| Metric                  | Description                              | Weight |
| ----------------------- | ---------------------------------------- | ------ |
| `correctness`           | Result matches expected output           | 40%    |
| `hallucination_penalty` | Count of phantom identifiers             | 25%    |
| `efficiency`            | Query cost / execution time              | 15%    |
| `grounding_score`       | % of references validated against schema | 20%    |

---

## 📁 Project Structure

```
agentx/
├── README.md
├── pyproject.toml
├── docker-compose.yml              # Multi-DB local environment
│
├── src/
│   ├── agentx/
│   │   ├── __init__.py
│   │   │
│   │   ├── core/                   # Core abstractions
│   │   │   ├── task.py             # Task definition models
│   │   │   ├── agent_interface.py  # Agent protocol/ABC
│   │   │   └── config.py           # Framework configuration
│   │   │
│   │   ├── infrastructure/         # ORM Infrastructure Layer
│   │   │   ├── database_manager.py # Multi-dialect engine management
│   │   │   ├── schema_inspector.py # SQLAlchemy + INFORMATION_SCHEMA
│   │   │   ├── fixture_loader.py   # Tiered bulk loading
│   │   │   └── dialects/           # Dialect-specific extensions
│   │   │       ├── bigquery.py
│   │   │       ├── snowflake.py
│   │   │       ├── postgres.py
│   │   │       └── duckdb.py
│   │   │
│   │   ├── sandbox/                # Agent Interaction Layer
│   │   │   ├── tool_registry.py    # Tool definitions
│   │   │   ├── tools/
│   │   │   │   ├── get_schema.py
│   │   │   │   ├── sample_rows.py
│   │   │   │   ├── search_docs.py
│   │   │   │   ├── validate_sql.py
│   │   │   │   └── execute_sql.py
│   │   │   └── session.py          # Agent session management
│   │   │
│   │   ├── validation/             # Anti-Hallucination Layer
│   │   │   ├── sql_parser.py       # AST extraction (sqlglot)
│   │   │   ├── hallucination_detector.py
│   │   │   ├── schema_validator.py
│   │   │   └── join_path_verifier.py
│   │   │
│   │   ├── evaluation/             # Raw SQL Evaluation Layer
│   │   │   ├── executor.py         # Raw SQL execution
│   │   │   ├── comparators/        # Result comparison strategies
│   │   │   │   ├── exact.py
│   │   │   │   ├── set_based.py
│   │   │   │   ├── fuzzy_numeric.py
│   │   │   │   └── schema_only.py
│   │   │   ├── scorer.py           # Multi-dimensional scoring
│   │   │   └── query_analyzer.py   # EXPLAIN plan capture
│   │   │
│   │   ├── logging/                # Observability Layer
│   │   │   ├── trace_logger.py     # Structured JSONL logging
│   │   │   ├── error_taxonomy.py   # Auto-classification
│   │   │   └── metrics.py          # Aggregated statistics
│   │   │
│   │   └── orchestrator/           # Task Orchestration
│   │       ├── runner.py           # Main evaluation loop
│   │       ├── environment.py      # Per-task env setup/teardown
│   │       └── reporter.py         # Results aggregation
│   │
├── tasks/                          # Task Definitions
│   ├── schemas/                    # JSON Schema definitions
│   ├── fixtures/                   # Test data (Parquet/JSON)
│   ├── tasks.yaml                  # Task registry
│   └── gold_queries/               # Expected SQL solutions
│
├── docs/                           # External documentation corpus
│   ├── bigquery/
│   ├── snowflake/
│   └── dialect_guides/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
└── scripts/
    ├── setup_databases.py
    ├── run_evaluation.py
    └── generate_report.py
```

---

## 🗄️ Supported Databases

| Database   | SQLAlchemy Dialect    | Status     |
| ---------- | --------------------- | ---------- |
| PostgreSQL | `postgresql+psycopg2` | ✅ Core    |
| SQLite     | `sqlite`              | ✅ Core    |
| DuckDB     | `duckdb_engine`       | ✅ Core    |
| BigQuery   | `bigquery`            | ✅ Cloud   |
| Snowflake  | `snowflake`           | ✅ Cloud   |
| ClickHouse | `clickhouse`          | 🔄 Planned |

---

## 🔄 Evaluation Flow

```
1. LOAD TASK
   └── Parse task definition (question, schema, expected output, difficulty)

2. SETUP ENVIRONMENT (ORM Layer)
   ├── Create database/schema
   ├── Load fixtures via FixtureLoader
   └── Snapshot schema via SchemaInspector

3. AGENT INTERACTION (Sandbox)
   ├── Agent calls GetSchema() → ORM introspection
   ├── Agent calls SampleRows(table) → ORM query
   ├── Agent calls ValidateSQL(sql) → HallucinationDetector
   │   └── Pre-execution validation against schema snapshot
   └── Agent submits final SQL

4. EXECUTION (Raw SQL Layer)
   ├── Execute agent SQL verbatim: connection.execute(text(sql))
   ├── Capture timing, query plan, result set
   └── Execute gold SQL for comparison

5. EVALUATION (Scorer)
   ├── Compare results via selected Comparator
   ├── Calculate hallucination penalty
   ├── Compute efficiency score
   └── Aggregate multi-dimensional score

6. LOGGING (Observability)
   ├── Write structured trace (JSONL)
   ├── Classify errors into taxonomy
   └── Update aggregate metrics

7. TEARDOWN (ORM Layer)
   └── Rollback/drop test database
```

---

## 📊 Error Taxonomy

| Category           | Description                      | Detection Method               |
| ------------------ | -------------------------------- | ------------------------------ |
| `SCHEMA_LINK`      | Wrong table/column selected      | SchemaValidator mismatch       |
| `JOIN_ERROR`       | Incorrect join path or condition | JoinPathVerifier + result diff |
| `SYNTAX_ERROR`     | SQL parse failure                | sqlglot parse exception        |
| `GROUNDING_ERROR`  | Phantom column/table/function    | HallucinationDetector          |
| `TRUNCATION_ERROR` | Context window overflow          | Token count monitoring         |
| `DATA_ANALYSIS`    | Wrong aggregation/filter logic   | Result comparison failure      |
| `DOC_MISINTERPRET` | Dialect syntax confusion         | Dialect-specific pattern match |

---

## 🆚 Comparison with Spider 2.0

| Dimension                   | Spider 2.0            | AgentX                                                                | Improvement             |
| --------------------------- | --------------------- | --------------------------------------------------------------------- | ----------------------- |
| **Hallucination Detection** | Post-execution only   | Pre-execution schema validation                                       | Structural prevention   |
| **Scoring**                 | Binary pass/fail      | Multi-dimensional (correctness, hallucination, efficiency, grounding) | Root-cause visibility   |
| **Dialect Management**      | Separate handling     | Unified SQLAlchemy abstraction                                        | Maintainability         |
| **Fixture Reproducibility** | Not specified         | ORM models + transactional rollback                                   | Deterministic isolation |
| **Error Analysis**          | Manual categorization | Automated taxonomy classification                                     | Scalable analysis       |
| **Nested Types**            | Major failure mode    | Hybrid introspection (ORM + INFORMATION_SCHEMA)                       | Enterprise support      |

---

## 🚀 Quick Start

```bash
# Clone repository
git clone https://github.com/ashcastelinocs124/AgentX-Hackathon.git
cd AgentX-Hackathon

# Install dependencies
pip install -e ".[dev]"

# Start local databases
docker-compose up -d

# Run evaluation
python scripts/run_evaluation.py --tasks tasks/tasks.yaml --agent your_agent
```

---

## 📅 Implementation Roadmap

### Phase 1: Foundation & Database Infrastructure (Weeks 1-2)

- `DatabaseManager` with SQLAlchemy engine factory
- PostgreSQL, SQLite, DuckDB support
- Docker Compose for local databases
- Configuration system

### Phase 2: Schema Introspection & Fixtures (Weeks 3-4)

- `SchemaInspector` using `sqlalchemy.inspect()`
- Hybrid introspection for nested types
- `FixtureLoader` with tiered loading
- Transactional setup/teardown

### Phase 3: Hallucination Detection (Weeks 5-6)

- SQL AST parser using `sqlglot`
- `SchemaValidator` and `HallucinationDetector`
- `JoinPathVerifier`

### Phase 4: Agent Sandbox (Weeks 7-8)

- Tool protocol definition
- GetSchema, SampleRows, ValidateSQL, ExecuteSQL tools
- Session management

### Phase 5: Evaluation Pipeline (Weeks 9-11)

- Raw SQL executor
- Result comparators (exact, set, fuzzy, schema-only)
- Multi-dimensional scorer
- Structured logging and error taxonomy

### Phase 6: Cloud & Advanced (Weeks 12-14)

- BigQuery and Snowflake dialects
- Cost estimation scoring
- dbt project introspection
- Metrics dashboard

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Contributions welcome! Please read our contributing guidelines before submitting PRs.

### Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/

# Run linting
ruff check src/
```

---

## 📚 References

- [Spider 2.0 Paper](https://arxiv.org/abs/2411.07763)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [sqlglot Documentation](https://sqlglot.com/)
