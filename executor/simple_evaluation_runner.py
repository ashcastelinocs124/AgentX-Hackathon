# This file defines SimpleEvaluationRunner.
#
# SimpleEvaluationRunner is a minimal end-to-end evaluation orchestrator.
# It integrates together:
#   - an agent (that generates SQL for a Task),
#   - a SQLExecutor,
#   - an optional QueryAnalyzer,
#   - a ResultComparator,
#   - a Scorer,
# and returns TaskResult objects for each Task.

from typing import Iterable, List, Optional, Any

from executor.data_structures import Task, TaskResult
from evaluation.data_structures import (
    ExecutionResult,
    ComparisonResult,
    MultiDimensionalScore,
    QueryPlan,
)

from evaluation.sql_executor import SQLExecutor          
from evaluation.query_analyzer import QueryAnalyzer      
from evaluation.result_comparator import ResultComparator 
from evaluation.scorer import Scorer                    


class SimpleEvaluationRunner:
    """
    A minimal evaluation runner that executes a sequence of Tasks end-to-end.

    Responsibilities for each Task:
        1. Ask the agent to generate SQL for the Task.
        2. Execute that SQL using SQLExecutor.
        3. Optionally obtain a QueryPlan using QueryAnalyzer.
        4. If expected_result is available:
               Compare actual vs expected using ResultComparator.
           Else:
               Create a dummy ComparisonResult.
        5. Compute a MultiDimensionalScore using Scorer.
        6. Return a TaskResult.
    """

    def __init__(
        self,
        agent: Any,
        executor: SQLExecutor,
        comparator: ResultComparator,
        scorer: Scorer,
        query_analyzer: Optional[QueryAnalyzer] = None,
    ) -> None:
        """
        Initialize the runner with all necessary dependencies.

        Parameters:
            agent          -> Object with a method `generate_sql(task: Task) -> str`.
                              (In phase 1, this can be a dummy agent that returns gold SQL.)
            executor       -> Concrete implementation of SQLExecutor (e.g., DBAPISQLExecutor).
            comparator     -> Concrete implementation of ResultComparator
                              (e.g., ExactMatchComparator).
            scorer         -> Concrete implementation of Scorer (e.g., DefaultScorer).
            query_analyzer -> Optional QueryAnalyzer used to obtain QueryPlan for scoring.
        """
        self.agent = agent
        self.executor = executor
        self.comparator = comparator
        self.scorer = scorer
        self.query_analyzer = query_analyzer

    def _make_dummy_comparison(self, message: str) -> ComparisonResult:
        """
        Create a ComparisonResult representing a non-comparable or failed case,
        e.g., when expected_result is missing or SQL execution failed.

        This keeps the rest of the pipeline from having to special-case None.
        """
        return ComparisonResult(
            is_correct=False,
            row_match_ratio=0.0,
            column_match_ratio=0.0,
            numeric_tolerance_ok=False,
            missing_columns=[],
            extra_columns=[],
            message=message,
        )

    def _make_zero_score(self, reason: str) -> MultiDimensionalScore:
        """
        Create a MultiDimensionalScore with all zeros, used in error cases.
        """
        return MultiDimensionalScore(
            correctness=0.0,
            efficiency=0.0,
            safety=0.0,
            overall=0.0,
            details={"reason": reason},
        )

    def run(self, tasks: Iterable[Task]) -> List[TaskResult]:
        """
        Run evaluation over a sequence of Tasks and return a list of TaskResult.
        """
        results: List[TaskResult] = []

        for task in tasks:
            # 1. Ask the agent to generate SQL for this Task
            generated_sql = self.agent.generate_sql(task)

            # 2. Execute the SQL
            execution: ExecutionResult = self.executor.execute(generated_sql)

            # 3. Optionally obtain a QueryPlan
            query_plan: Optional[QueryPlan] = None
            if self.query_analyzer is not None and execution.error is None:
                query_plan = self.query_analyzer.analyze(generated_sql)

            # 4. Compare actual vs expected (if we have expected_result)
            if execution.error is not None:
                # Execution failed; produce a dummy ComparisonResult and zero score
                comparison = self._make_dummy_comparison(
                    message=f"SQL execution failed: {execution.error}"
                )
                score = self._make_zero_score(reason="execution_error")
            elif task.expected_result is None:
                # Cannot compare without ground-truth expected_result
                comparison = self._make_dummy_comparison(
                    message="No expected_result provided for this task; "
                            "skipping correctness comparison."
                )
                # You can decide here: maybe still score efficiency based on execution/query_plan.
                score = self.scorer.score(
                    comparison=comparison,
                    timing_ms=execution.timing_ms,
                    query_plan=query_plan,
                    hallucination_info=None,
                )
            else:
                # Happy path: execution succeeded and we have an expected_result
                comparison: ComparisonResult = self.comparator.compare(
                    actual=execution.rows,
                    expected=task.expected_result,
                )
                score: MultiDimensionalScore = self.scorer.score(
                    comparison=comparison,
                    timing_ms=execution.timing_ms,
                    query_plan=query_plan,
                    hallucination_info=None,  # hook Dev 2's output here later
                )

            # 5. put everything into a TaskResult
            task_result = TaskResult(
                task=task,
                generated_sql=generated_sql,
                execution=execution,
                comparison=comparison,
                score=score,
                query_plan=query_plan,
            )

            results.append(task_result)

        return results