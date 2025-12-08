'''
defines the SQLExecutor interface and a concrete implementation
SQLExecutor - Abstract base class (interface) for executing SQL.
DBAPISQLExecutor - Concrete implementation that uses a DB-API connection object.
'''


from abc import ABC, abstractmethod
import time
from typing import Any

from data_structures import ExecutionResult  


class SQLExecutor(ABC):
    """
    Interface for components that can execute SQL queries.

    Responsibilities:
    - Execute a given SQL string against a database.
    - Measure how long the query took.
    - Return rows and column names on success.
    - Capture any errors (instead of raising them) inside ExecutionResult.
    """

    @abstractmethod
    def execute(self, sql: str) -> ExecutionResult:
        raise NotImplementedError


class DBAPISQLExecutor(SQLExecutor):
    """
    Concrete implementation of SQLExecutor using a standard Python DB-API
    2.0 connection object.

    Assumptions:
        - self.connection is a DB-API connection, e.g.:
            - sqlite3.Connection
            - psycopg2.extensions.connection
            - mysql.connector.connection.MySQLConnection
        - The connection's cursor supports:
            - cursor.execute(sql)
            - cursor.fetchall()
            - cursor.description (tuple of column metadata)
    """

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def execute(self, sql: str) -> ExecutionResult:
        """
        Execute the given SQL query using the DB-API connection and return
        an ExecutionResult containing:
            - rows
            - column names
            - execution time (ms)
            - any error that occurred

        captures exceptions in ExecutionResult.error so the caller can handle them.
        """
        start_time = time.perf_counter()

        rows = []
        columns: list[str] = []
        error: Exception | None = None

        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            # Fetch all rows from the cursor
            rows = cursor.fetchall()

            # Extract column names from cursor.description (if available)
            if cursor.description is not None:
                columns = [col[0] for col in cursor.description]
            else:
                columns = []

        except Exception as exc:
            # Capture the exception instead of raising it
            error = exc

        finally:
            # Always compute timing, even if an error occurred
            end_time = time.perf_counter()
            timing_ms = (end_time - start_time) * 1000.0

            # Make sure we close the cursor if it was created
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    # Ignore cursor close errors; they shouldn't crash evaluation
                    pass

        return ExecutionResult(
            rows=rows,
            columns=columns,
            timing_ms=timing_ms,
            error=error,
        )