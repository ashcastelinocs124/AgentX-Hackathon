# AgentX: Work Division & Implementation Plan

> Team coordination guide for 4 developers implementing the AgentX evaluation framework.

---

## 📊 Overview

| Developer | Domain              | Core Responsibility                                     |
| --------- | ------------------- | ------------------------------------------------------- |
| **Dev 1** | Infrastructure Lead | Database connections, ORM layer, fixtures               |
| **Dev 2** | Validation Lead     | SQL parsing, hallucination detection, schema validation |
| **Dev 3** | Agent & Tools Lead  | Sandbox environment, tool APIs, session management      |
| **Dev 4** | Evaluation Lead     | Execution, comparison, scoring, logging                 |

---

## 🔗 Dependency Graph & Execution Order

```
                    WEEK 1-2              WEEK 3-4              WEEK 5-8              WEEK 9-14
                    ────────              ────────              ────────              ─────────

     ┌─────────────────────────────────────────────────────────────────────────────────────────┐
     │ DEV 1: INFRASTRUCTURE                                                                   │
     │ ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │
     │ │ DatabaseMgr  │───►│ SchemaInspect│───►│ FixtureLoader│───►│ Cloud Dialect│           │
     │ │ (BLOCKING)   │    │              │    │              │    │ BigQuery/Snow│           │
     │ └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘           │
     └─────────┬───────────────────┬───────────────────────────────────────────────────────────┘
               │                   │
               │ UNBLOCKS          │ UNBLOCKS
               ▼                   ▼
     ┌─────────────────────────────────────────────────────────────────────────────────────────┐
     │ DEV 2: VALIDATION (Can start Week 2)                                                    │
     │          ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                       │
     │          │ SQL Parser   │───►│ Schema       │───►│ Hallucinate  │                       │
     │          │ (sqlglot)    │    │ Validator    │    │ Detector     │                       │
     │          └──────────────┘    └──────────────┘    └──────────────┘                       │
     └─────────────────────────────────────┬───────────────────────────────────────────────────┘
                                           │
               ┌───────────────────────────┤ UNBLOCKS
               │                           │
               ▼                           ▼
     ┌─────────────────────────────────────────────────────────────────────────────────────────┐
     │ DEV 3: AGENT SANDBOX (Can start Week 2)                                                 │
     │          ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                       │
     │          │ Tool Protocol│───►│ Tool Impls   │───►│ Session Mgmt │                       │
     │          │ & Registry   │    │ (5 tools)    │    │ Multi-turn   │                       │
     │          └──────────────┘    └──────────────┘    └──────────────┘                       │
     └─────────────────────────────────────┬───────────────────────────────────────────────────┘
                                           │
                                           │ UNBLOCKS
                                           ▼
     ┌─────────────────────────────────────────────────────────────────────────────────────────┐
     │ DEV 4: EVALUATION (Can start Week 3)                                                    │
     │                    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐             │
     │                    │ Executor     │───►│ Comparators  │───►│ Scorer +     │             │
     │                    │ + Analyzer   │    │ (4 types)    │    │ Orchestrator │             │
     │                    └──────────────┘    └──────────────┘    └──────────────┘             │
     └─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 👨‍💻 Developer 1: Infrastructure Lead

### Scope

All database connectivity, ORM management, schema introspection, and fixture loading.

### Files Owned

```
src/agentx/
├── core/
│   └── config.py                   # Framework configuration
├── infrastructure/
│   ├── __init__.py
│   ├── database_manager.py         # Multi-dialect engine factory
│   ├── schema_inspector.py         # SQLAlchemy + INFORMATION_SCHEMA
│   ├── fixture_loader.py           # Tiered bulk loading
│   └── dialects/
│       ├── __init__.py
│       ├── base.py                 # Dialect interface
│       ├── postgres.py
│       ├── sqlite.py
│       ├── duckdb.py
│       ├── bigquery.py             # Phase 6
│       └── snowflake.py            # Phase 6
docker-compose.yml
pyproject.toml                      # Project dependencies
```

### Deliverables by Week

| Week  | Deliverable                         | Exit Criteria                                   |
| ----- | ----------------------------------- | ----------------------------------------------- |
| 1     | `DatabaseManager` + config          | Connect to Postgres/SQLite/DuckDB               |
| 2     | Docker Compose + connection pooling | `docker-compose up` works, pooling tested       |
| 3     | `SchemaInspector` basic             | Enumerate tables/columns for all 3 dialects     |
| 4     | Nested type introspection           | STRUCT/ARRAY types handled via raw SQL fallback |
| 5-6   | `FixtureLoader`                     | Load <100K via ORM, >100K via bulk              |
| 7-8   | Transactional rollback              | Per-test isolation verified                     |
| 12-14 | BigQuery/Snowflake dialects         | Cloud connections + introspection working       |

### Interfaces to Expose

```python
# Other devs depend on these interfaces

class DatabaseManager:
    def get_engine(self, dialect: str) -> Engine: ...
    def get_connection(self, dialect: str) -> Connection: ...

class SchemaInspector:
    def get_tables(self) -> List[str]: ...
    def get_columns(self, table: str) -> List[ColumnInfo]: ...
    def get_schema_snapshot(self) -> SchemaSnapshot: ...

class FixtureLoader:
    def load(self, fixtures: Dict[str, Any]) -> None: ...
    def teardown(self) -> None: ...
```

### Dependencies

- **None** (foundational layer)

### Blocks

- Dev 2 (needs `SchemaSnapshot` from Week 3)
- Dev 3 (needs `get_connection` from Week 1)
- Dev 4 (needs `get_connection` from Week 1)

---

## 👨‍💻 Developer 2: Validation Lead

### Scope

SQL parsing, AST extraction, schema validation, hallucination detection, JOIN path verification.

### Files Owned

```
src/agentx/
├── validation/
│   ├── __init__.py
│   ├── sql_parser.py               # sqlglot AST extraction
│   ├── schema_validator.py         # Column/table existence checks
│   ├── hallucination_detector.py   # Phantom identifier detection
│   ├── join_path_verifier.py       # FK/relationship validation
│   └── models.py                   # ValidationResult, HallucinationReport
```

### Deliverables by Week

| Week | Deliverable                 | Exit Criteria                                     |
| ---- | --------------------------- | ------------------------------------------------- |
| 2    | `sql_parser.py`             | Parse SQL → extract tables, columns, functions    |
| 3    | `schema_validator.py`       | Given SQL + SchemaSnapshot, list invalid refs     |
| 4    | `hallucination_detector.py` | Detect phantom columns/tables/functions           |
| 5    | `join_path_verifier.py`     | Validate JOIN conditions against FK relationships |
| 6    | Multi-dialect parsing       | Handle BigQuery/Snowflake syntax variations       |
| 7-8  | Integration with tools      | `ValidateSQL` tool uses detector                  |

### Interfaces to Expose

```python
class SQLParser:
    def parse(self, sql: str, dialect: str) -> ParsedSQL: ...
    def extract_identifiers(self, sql: str) -> IdentifierSet: ...

class SchemaValidator:
    def validate(self, identifiers: IdentifierSet, schema: SchemaSnapshot) -> ValidationResult: ...

class HallucinationDetector:
    def detect(self, sql: str, schema: SchemaSnapshot) -> HallucinationReport: ...
    # HallucinationReport includes: phantom_columns, phantom_tables, phantom_functions, score

class JoinPathVerifier:
    def verify(self, sql: str, schema: SchemaSnapshot) -> JoinValidationResult: ...
```

### Dependencies

- Dev 1: `SchemaSnapshot` (available Week 3)

### Blocks

- Dev 3: `ValidateSQL` tool implementation (Week 5)
- Dev 4: Hallucination penalty scoring (Week 9)

---

## 👨‍💻 Developer 3: Agent & Tools Lead

### Scope

Agent sandbox environment, tool protocol, all tool implementations, session management.

### Files Owned

```
src/agentx/
├── core/
│   ├── task.py                     # Task definition models
│   └── agent_interface.py          # Agent protocol/ABC
├── sandbox/
│   ├── __init__.py
│   ├── tool_registry.py            # Tool registration & discovery
│   ├── tool_protocol.py            # Base tool interface
│   ├── session.py                  # Agent session management
│   └── tools/
│       ├── __init__.py
│       ├── base.py                 # BaseTool ABC
│       ├── get_schema.py
│       ├── sample_rows.py
│       ├── search_docs.py
│       ├── validate_sql.py
│       └── execute_sql.py
docs/                               # Documentation corpus for SearchDocs
├── bigquery/
├── snowflake/
└── dialect_guides/
```

### Deliverables by Week

| Week | Deliverable                    | Exit Criteria                             |
| ---- | ------------------------------ | ----------------------------------------- |
| 2    | `tool_protocol.py` + `base.py` | Tool interface defined                    |
| 3    | `get_schema.py`                | Returns schema from Dev 1's inspector     |
| 4    | `sample_rows.py`               | Returns N rows from table                 |
| 5    | `validate_sql.py`              | Integrates Dev 2's hallucination detector |
| 6    | `execute_sql.py`               | Raw execution with result capture         |
| 7    | `search_docs.py`               | RAG over documentation corpus             |
| 8    | `session.py`                   | Multi-turn session with state             |
| 9-11 | Agent integration tests        | Mock agent + real agent tests             |

### Interfaces to Expose

```python
class BaseTool(ABC):
    name: str
    description: str
    @abstractmethod
    def execute(self, **kwargs) -> ToolResult: ...

class ToolRegistry:
    def register(self, tool: BaseTool) -> None: ...
    def get(self, name: str) -> BaseTool: ...
    def list_tools(self) -> List[ToolInfo]: ...

class AgentSession:
    def __init__(self, task: Task, tools: ToolRegistry): ...
    def handle_tool_call(self, tool_name: str, **kwargs) -> ToolResult: ...
    def submit_final_sql(self, sql: str) -> SubmissionResult: ...
    def get_trace(self) -> SessionTrace: ...

class AgentInterface(Protocol):
    def solve(self, task: Task, session: AgentSession) -> str: ...
```

### Dependencies

- Dev 1: `DatabaseManager.get_connection()` (Week 1), `SchemaInspector` (Week 3)
- Dev 2: `HallucinationDetector` (Week 4)

### Blocks

- Dev 4: Orchestrator needs `AgentSession` (Week 8)

---

## 👨‍💻 Developer 4: Evaluation Lead

### Scope

SQL execution, result comparison, multi-dimensional scoring, logging, error taxonomy, orchestration.

### Files Owned

```
src/agentx/
├── evaluation/
│   ├── __init__.py
│   ├── executor.py                 # Raw SQL execution
│   ├── query_analyzer.py           # EXPLAIN plan capture
│   ├── scorer.py                   # Multi-dimensional scoring
│   └── comparators/
│       ├── __init__.py
│       ├── base.py                 # Comparator interface
│       ├── exact.py
│       ├── set_based.py
│       ├── fuzzy_numeric.py
│       └── schema_only.py
├── logging/
│   ├── __init__.py
│   ├── trace_logger.py             # Structured JSONL logging
│   ├── error_taxonomy.py           # Auto-classification
│   └── metrics.py                  # Aggregated statistics
├── orchestrator/
│   ├── __init__.py
│   ├── runner.py                   # Main evaluation loop
│   ├── environment.py              # Per-task env setup/teardown
│   └── reporter.py                 # Results aggregation & output
scripts/
├── setup_databases.py
├── run_evaluation.py
└── generate_report.py
```

### Deliverables by Week

| Week  | Deliverable                    | Exit Criteria                            |
| ----- | ------------------------------ | ---------------------------------------- |
| 3     | `executor.py`                  | Execute raw SQL, capture results/timing  |
| 4     | `query_analyzer.py`            | EXPLAIN plan capture per dialect         |
| 5-6   | `comparators/*`                | All 4 comparison strategies              |
| 7     | `trace_logger.py`              | JSONL trace per evaluation               |
| 8     | `error_taxonomy.py`            | Auto-classify into 7 categories          |
| 9     | `scorer.py`                    | Multi-dimensional scoring                |
| 10    | `runner.py` + `environment.py` | Full orchestration loop                  |
| 11    | `reporter.py`                  | Aggregate results, markdown/JSON output  |
| 12-14 | Dashboard + CLI                | `run_evaluation.py` CLI, cost estimation |

### Interfaces to Expose

```python
class SQLExecutor:
    def execute(self, sql: str, connection: Connection) -> ExecutionResult: ...
    # ExecutionResult includes: rows, columns, timing_ms, error

class QueryAnalyzer:
    def analyze(self, sql: str, connection: Connection) -> QueryPlan: ...

class ResultComparator(ABC):
    @abstractmethod
    def compare(self, actual: DataFrame, expected: DataFrame) -> ComparisonResult: ...

class Scorer:
    def score(self,
              comparison: ComparisonResult,
              hallucination: HallucinationReport,
              timing: float,
              query_plan: QueryPlan) -> MultiDimensionalScore: ...

class EvaluationRunner:
    def run(self, tasks: List[Task], agent: AgentInterface) -> EvaluationReport: ...

class ErrorTaxonomy:
    def classify(self, error: Exception, context: EvalContext) -> ErrorCategory: ...
```

### Dependencies

- Dev 1: `DatabaseManager.get_connection()` (Week 1)
- Dev 2: `HallucinationReport` (Week 4)
- Dev 3: `AgentSession`, `AgentInterface` (Week 8)

### Blocks

- **None** (final integration layer)

---

## 📅 Coordination Schedule

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         WEEKLY SYNC POINTS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Week 1:  Dev 1 defines DatabaseManager interface                           │
│           All devs review, agree on connection API                          │
│                                                                             │
│  Week 2:  Dev 1 delivers working connections                                │
│           Dev 2 starts SQL parser (no deps)                                 │
│           Dev 3 starts tool protocol (no deps)                              │
│                                                                             │
│  Week 3:  Dev 1 delivers SchemaSnapshot                                     │
│           Dev 2 integrates for validation                                   │
│           Dev 4 starts executor                                             │
│                                                                             │
│  Week 5:  ⭐ INTEGRATION CHECKPOINT                                          │
│           Dev 2 delivers HallucinationDetector                              │
│           Dev 3 integrates into ValidateSQL tool                            │
│           Dev 4 integrates into scorer                                      │
│                                                                             │
│  Week 8:  ⭐ INTEGRATION CHECKPOINT                                          │
│           Dev 3 delivers AgentSession                                       │
│           Dev 4 integrates into orchestrator                                │
│           Full pipeline test with mock agent                                │
│                                                                             │
│  Week 11: ⭐ END-TO-END TESTING                                              │
│           All devs: integration tests, bug fixes                            │
│                                                                             │
│  Week 14: 🚀 RELEASE                                                         │
│           Cloud dialects, documentation, final testing                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Shared Models (Interface Contracts)

All developers must agree on these shared models. Create this file first: `src/agentx/core/models.py`

```python
"""Shared models for cross-developer interface contracts."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime


# ============================================================
# SCHEMA MODELS (Dev 1 produces, Dev 2/3/4 consume)
# ============================================================

@dataclass
class ColumnInfo:
    """Column metadata from schema introspection."""
    name: str
    dtype: str
    nullable: bool
    primary_key: bool = False
    foreign_key: Optional[str] = None  # "table.column" format


@dataclass
class TableInfo:
    """Table metadata from schema introspection."""
    name: str
    columns: List[ColumnInfo]
    row_count: Optional[int] = None


@dataclass
class SchemaSnapshot:
    """Complete schema snapshot for validation."""
    dialect: str
    database: str
    tables: Dict[str, TableInfo]
    captured_at: datetime = field(default_factory=datetime.utcnow)

    def has_table(self, name: str) -> bool:
        return name.lower() in {t.lower() for t in self.tables}

    def has_column(self, table: str, column: str) -> bool:
        tbl = self.tables.get(table)
        if not tbl:
            return False
        return column.lower() in {c.name.lower() for c in tbl.columns}


# ============================================================
# VALIDATION MODELS (Dev 2 produces, Dev 3/4 consume)
# ============================================================

@dataclass
class IdentifierSet:
    """Extracted SQL identifiers."""
    tables: List[str]
    columns: List[str]  # "table.column" or just "column"
    functions: List[str]
    aliases: Dict[str, str]  # alias -> actual name


@dataclass
class HallucinationReport:
    """Report of phantom identifiers in SQL."""
    phantom_tables: List[str]
    phantom_columns: List[str]
    phantom_functions: List[str]
    total_hallucinations: int = 0
    hallucination_score: float = 0.0  # 0.0 = none, 1.0 = all phantom

    def __post_init__(self):
        self.total_hallucinations = (
            len(self.phantom_tables) +
            len(self.phantom_columns) +
            len(self.phantom_functions)
        )


@dataclass
class ValidationResult:
    """Result of SQL validation against schema."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    hallucination_report: Optional[HallucinationReport] = None


# ============================================================
# EXECUTION MODELS (Dev 4 produces)
# ============================================================

@dataclass
class ExecutionResult:
    """Result of SQL execution."""
    success: bool
    rows: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    timing_ms: float
    error: Optional[str] = None


@dataclass
class QueryPlan:
    """Query execution plan from EXPLAIN."""
    raw_plan: str
    estimated_cost: Optional[float] = None
    estimated_rows: Optional[int] = None


# ============================================================
# SCORING MODELS (Dev 4 produces)
# ============================================================

class ErrorCategory(Enum):
    """Error taxonomy categories."""
    SCHEMA_LINK = "schema_link"
    JOIN_ERROR = "join_error"
    SYNTAX_ERROR = "syntax_error"
    GROUNDING_ERROR = "grounding_error"
    TRUNCATION_ERROR = "truncation_error"
    DATA_ANALYSIS = "data_analysis"
    DOC_MISINTERPRET = "doc_misinterpret"
    UNKNOWN = "unknown"


@dataclass
class ComparisonResult:
    """Result of comparing actual vs expected results."""
    match: bool
    match_score: float  # 0.0 to 1.0
    strategy: str  # "exact", "set_based", "fuzzy", "schema_only"
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MultiDimensionalScore:
    """Multi-dimensional evaluation score."""
    correctness: float          # 0.0 to 1.0, weight 40%
    hallucination_penalty: float  # 0.0 to 1.0, weight 25%
    efficiency: float           # 0.0 to 1.0, weight 15%
    grounding: float            # 0.0 to 1.0, weight 20%

    @property
    def weighted_total(self) -> float:
        return (
            self.correctness * 0.40 +
            (1.0 - self.hallucination_penalty) * 0.25 +
            self.efficiency * 0.15 +
            self.grounding * 0.20
        )


# ============================================================
# TASK MODELS (Shared)
# ============================================================

@dataclass
class Task:
    """Evaluation task definition."""
    id: str
    question: str
    database: str
    dialect: str
    difficulty: str  # "easy", "medium", "hard", "enterprise"
    gold_sql: str
    expected_result: Optional[List[Dict[str, Any]]] = None
    tags: List[str] = field(default_factory=list)


# ============================================================
# TOOL MODELS (Dev 3 produces, Dev 4 consumes)
# ============================================================

@dataclass
class ToolResult:
    """Result from tool execution."""
    success: bool
    data: Any
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SessionTrace:
    """Trace of agent session."""
    task_id: str
    tool_calls: List[Dict[str, Any]]
    final_sql: Optional[str]
    started_at: datetime
    ended_at: Optional[datetime] = None
    total_tool_calls: int = 0
```

---

## 📊 Summary Table

| Developer | Weeks | Key Deliverables                                                    | Depends On                                     | Blocks      |
| --------- | ----- | ------------------------------------------------------------------- | ---------------------------------------------- | ----------- |
| **Dev 1** | 1-14  | DatabaseManager, SchemaInspector, FixtureLoader, Cloud Dialects     | None                                           | Dev 2, 3, 4 |
| **Dev 2** | 2-8   | SQLParser, SchemaValidator, HallucinationDetector, JoinPathVerifier | Dev 1 (Week 3)                                 | Dev 3, 4    |
| **Dev 3** | 2-11  | Tool Protocol, 5 Tools, AgentSession                                | Dev 1 (Week 1), Dev 2 (Week 4)                 | Dev 4       |
| **Dev 4** | 3-14  | Executor, Comparators, Scorer, Logger, Orchestrator, CLI            | Dev 1 (Week 1), Dev 2 (Week 4), Dev 3 (Week 8) | None        |

---

## 🔑 Critical Path

```
Dev 1: DatabaseManager (Week 1) → SchemaSnapshot (Week 3)
                ↓
Dev 2: SchemaValidator (Week 3) → HallucinationDetector (Week 4)
                ↓
Dev 3: ValidateSQL Tool (Week 5) → AgentSession (Week 8)
                ↓
Dev 4: Orchestrator (Week 10) → Full Pipeline (Week 11)
```

---

## ✅ First Week Kickoff Checklist

- [ ] All devs clone repo and set up local environment
- [ ] Dev 1 creates initial `pyproject.toml` with core dependencies
- [ ] All devs review and approve `src/agentx/core/models.py` (shared models)
- [ ] Dev 1 defines `DatabaseManager` interface, shares for review
- [ ] Set up weekly sync meetings (suggest: Mon/Thu)
- [ ] Create GitHub Projects board with tasks per developer
- [ ] Agree on branching strategy (suggest: feature branches → main)

---

## 🛠️ Development Guidelines

### Branching Strategy

```
main                    # Protected, requires PR review
├── dev/1/infrastructure  # Dev 1's feature branch
├── dev/2/validation      # Dev 2's feature branch
├── dev/3/sandbox         # Dev 3's feature branch
└── dev/4/evaluation      # Dev 4's feature branch
```

### PR Review Policy

- Each PR requires review from **at least 1 other developer**
- Interface changes require review from **all affected developers**
- Integration checkpoints (Week 5, 8, 11) require **all 4 approvals**

### Testing Requirements

- Unit tests required for all public interfaces
- Integration tests at each checkpoint
- E2E tests before release (Week 14)

---

## 📚 Resources

- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [sqlglot Documentation](https://sqlglot.com/)
- [pytest Documentation](https://docs.pytest.org/)
- [Spider 2.0 Paper](https://arxiv.org/abs/2411.07763)
