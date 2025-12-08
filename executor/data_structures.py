# orchestrator/types.py
#
# This file defines:
#   - Task:       input specification for a single evaluation case
#   - TaskResult: output of evaluating one Task through the pipeline
#
# These dataclasses are used by EvaluationRunners and higher-level
# reporting/aggregation logic.

from dataclasses import dataclass
from typing import Any, Optional

from evaluation.data_structures import (
    ExecutionResult,
    ComparisonResult,
    MultiDimensionalScore,
    QueryPlan,
)


@dataclass
class Task:
    """
    Represents a single evaluation task for the text-to-SQL system.

    Typical usage:
        A Task describes:
            - the natural language question,
            - optional gold SQL (ground truth query),
            - the expected result set (for comparison),
            - optional metadata (dataset name, db/schema name, etc.).

    Fields:
        task_id         -> Unique identifier for this task (e.g., "q_001").
        question        -> Natural language question given to the agent.
        sql_gold        -> Optional gold/expected SQL query, if available.
        expected_result -> Ground-truth result corresponding to the gold SQL.
                           This is what the comparator will use.
        metadata        -> Optional dictionary for any extra info
                           (dataset, tags, difficulty, etc.).
    """
    task_id: str
    question: str
    sql_gold: Optional[str] = None
    expected_result: Optional[Any] = None
    metadata: Optional[dict] = None


@dataclass
class TaskResult:
    """
    Captures the full outcome of evaluating a single Task.

    Fields:
        task           -> The original Task that was evaluated.
        generated_sql  -> The SQL produced by the agent for this task.
        execution      -> ExecutionResult from running generated_sql.
        comparison     -> ComparisonResult from comparing actual vs expected.
        score          -> MultiDimensionalScore for this task.
        query_plan     -> Optional QueryPlan used for efficiency analysis.
    """
    task: Task
    generated_sql: str
    execution: ExecutionResult
    comparison: ComparisonResult
    score: MultiDimensionalScore
    query_plan: Optional[QueryPlan] = None