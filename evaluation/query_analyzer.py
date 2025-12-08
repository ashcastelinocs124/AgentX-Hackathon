'''
# A QueryAnalyzer is responsible for:
- Running EXPLAIN or EXPLAIN ANALYZE on a SQL query
- Capturing the database's execution plan
- Returning a structured QueryPlan dataclass
'''


from abc import ABC, abstractmethod
from typing import Any, Dict
import time

from data_structures import QueryPlan  



class QueryAnalyzer(ABC):
    """
    Interface for components that analyze how a SQL query is executed.

    Responsibilities:
        - Run EXPLAIN or EXPLAIN ANALYZE on a SQL query.
        - Return a QueryPlan describing how the database executes the query.
    """

    @abstractmethod
    def analyze(self, sql: str) -> QueryPlan:
        raise NotImplementedError




class DBAPIQueryAnalyzer(QueryAnalyzer):
    """
    Concrete implementation of QueryAnalyzer using a DB-API 2.0 connection.

    This implementation uses:
        - EXPLAIN for planning-only analysis
        - EXPLAIN ANALYZE optionally (if enabled)
    """

    def __init__(self, connection: Any, use_analyze: bool = False) -> None:
        """
        Initialize the analyzer.

        Parameters:
            connection  -> DB-API compatible connection object
            use_analyze -> If True, runs EXPLAIN ANALYZE instead of EXPLAIN
                           (runs the actual query and measures real performance)
        """
        self.connection = connection
        self.use_analyze = use_analyze

    def analyze(self, sql: str) -> QueryPlan:
        """
        Run EXPLAIN or EXPLAIN ANALYZE on the given SQL query and return a QueryPlan.

        This method NEVER raises an exception for EXPLAIN failures.
        All errors are captured into the QueryPlan.notes field.
        """

        # Choose EXPLAIN mode
        explain_prefix = "EXPLAIN ANALYZE" if self.use_analyze else "EXPLAIN"
        explain_sql = f"{explain_prefix} {sql}"

        raw_plan = None
        estimated_cost = None
        estimated_rows = None
        notes: Dict[str, Any] = {}

        cursor = None

        try:
            cursor = self.connection.cursor()
            cursor.execute(explain_sql)

            plan_rows = cursor.fetchall()

            # Store the raw output directly
            raw_plan = plan_rows

            # Different DBs return different EXPLAIN formats.

            if plan_rows and isinstance(plan_rows[0][0], str):
                plan_text = "\n".join(row[0] for row in plan_rows)
                notes["plan_text"] = plan_text

            if plan_rows and isinstance(plan_rows[0][0], dict):
                plan_json = plan_rows[0][0]
                raw_plan = plan_json
                estimated_cost = plan_json.get("Plan", {}).get("Total Cost")
                estimated_rows = plan_json.get("Plan", {}).get("Plan Rows")
                notes["plan_json"] = plan_json

            #metadata
            notes["db_driver"] = type(self.connection).__name__
            notes["used_analyze"] = self.use_analyze

        except Exception as exc:
            # If EXPLAIN itself fails, we still return a QueryPlan so the rest of the pipeline doesn't crash.
            raw_plan = None
            estimated_cost = None
            estimated_rows = None
            notes["error"] = str(exc)

        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

        return QueryPlan(
            raw_plan=raw_plan,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            notes=notes,
        )
    