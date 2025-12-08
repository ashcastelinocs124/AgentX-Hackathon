
from dataclasses import dataclass
from typing import Any, List, Optional, Dict



@dataclass
class ExecutionResult:
    """
    Captures the outcome of executing an SQL query.

    Used by:
        - SQLExecutor (output)
        - EvaluationRunner (to decide next steps)
        - Comparators (comparing actual vs expected rows)
        - Logging & reporting

    What it contains:
        rows        -> Actual result set returned by the DB (e.g., DataFrame or list of tuples)
        columns     -> Column names returned by the DB cursor
        timing_ms   -> Execution time of the SQL query in milliseconds
        error       -> Exception object if SQL execution failed; None otherwise
    """
    rows: Any
    columns: List[str]
    timing_ms: float
    error: Optional[Exception]



@dataclass
class QueryPlan:
    """
    Stores the database's execution plan for a SQL query.

    Used by:
        - QueryAnalyzer (output)
        - Scorer (efficiency scoring)
        - Logging & debugging

    What it contains:
        raw_plan        -> Raw output of EXPLAIN or EXPLAIN ANALYZE (could be text or JSON)
        estimated_cost  -> Estimated computational cost assigned by the query planner
        estimated_rows  -> Estimated number of rows the DB expects to process
        notes           -> Additional metadata (join types, index usage, etc.)
    """
    raw_plan: Any
    estimated_cost: Optional[float]
    estimated_rows: Optional[float]
    notes: Dict[str, Any]



@dataclass
class ComparisonResult:
    """
    Represents the result of comparing the agent's output vs expected output.

    Used by:
        - ResultComparator implementations
        - Scorer (correctness computation)
        - Runner (to determine success/failure)
        - Reporting

    What it contains:
        is_correct          -> Overall correctness on a binary scale
        row_match_ratio     -> Fraction of rows that match (0 to 1)
        column_match_ratio  -> Fraction of expected columns present
        numeric_tolerance_ok-> True if numeric differences are within allowed tolerance
        missing_columns     -> Columns expected but not returned by the agent query
        extra_columns       -> Columns returned by the agent but not expected
        message             -> Human-readable explanation for debugging
    """
    is_correct: bool
    row_match_ratio: float
    column_match_ratio: float
    numeric_tolerance_ok: bool
    missing_columns: List[str]
    extra_columns: List[str]
    message: str



@dataclass
class MultiDimensionalScore:
    """
    Final scoring output for a single task across multiple evaluation dimensions.

    Used by:
        - Scorer (output)
        - EvaluationRunner (task result assembly)
        - Reporter & Metrics components

    What it contains:
        correctness -> Score based on comparison result (0 to 1)
        efficiency  -> Score based on timing + query plan quality
        safety      -> Score penalizing hallucinations or unsafe SQL
        overall     -> Combined weighted score
    """
    correctness: float
    efficiency: float
    safety: float
    overall: float