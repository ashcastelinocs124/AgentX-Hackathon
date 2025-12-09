import sqlglot
from typing import Any, List, Optional, Set
from dataclasses import dataclass
from .models import IdentifierSet


# Supported dialects mapping to sqlglot dialect names
DIALECT_MAP = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "sqlite": "sqlite",
    "duckdb": "duckdb",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "clickhouse": "clickhouse",
    "mysql": "mysql",
    "": None,  # Auto-detect
}

# BigQuery-specific functions that are valid
BIGQUERY_FUNCTIONS: Set[str] = {
    "SAFE_DIVIDE", "SAFE_MULTIPLY", "SAFE_NEGATE", "SAFE_ADD", "SAFE_SUBTRACT",
    "DATE_DIFF", "DATE_ADD", "DATE_SUB", "DATE_TRUNC", "DATETIME_DIFF",
    "TIMESTAMP_DIFF", "TIMESTAMP_ADD", "TIMESTAMP_SUB", "TIMESTAMP_TRUNC",
    "PARSE_DATE", "PARSE_DATETIME", "PARSE_TIMESTAMP", "FORMAT_DATE",
    "ARRAY_AGG", "ARRAY_LENGTH", "ARRAY_TO_STRING", "GENERATE_ARRAY",
    "STRUCT", "UNNEST", "ARRAY", "CURRENT_DATE", "CURRENT_TIMESTAMP",
    "IFNULL", "NULLIF", "COALESCE", "IF", "CASE",
    "REGEXP_CONTAINS", "REGEXP_EXTRACT", "REGEXP_REPLACE",
    "JSON_EXTRACT", "JSON_EXTRACT_SCALAR", "JSON_QUERY", "JSON_VALUE",
    "ST_GEOGPOINT", "ST_DISTANCE", "ST_CONTAINS", "ST_INTERSECTS",
    "APPROX_COUNT_DISTINCT", "APPROX_QUANTILES", "APPROX_TOP_COUNT",
    "FARM_FINGERPRINT", "MD5", "SHA256", "SHA512",
    "NET.IP_FROM_STRING", "NET.SAFE_IP_FROM_STRING", "NET.IP_TO_STRING",
}

# Snowflake-specific functions that are valid
SNOWFLAKE_FUNCTIONS: Set[str] = {
    "DATEADD", "DATEDIFF", "DATE_TRUNC", "DATE_PART", "DAYNAME", "MONTHNAME",
    "TIMEADD", "TIMEDIFF", "TIMESTAMPADD", "TIMESTAMPDIFF",
    "TO_DATE", "TO_TIMESTAMP", "TO_TIME", "TO_CHAR", "TO_VARCHAR",
    "TRY_TO_DATE", "TRY_TO_TIMESTAMP", "TRY_TO_NUMBER",
    "ARRAY_AGG", "ARRAY_SIZE", "ARRAY_SLICE", "ARRAY_CAT", "ARRAY_COMPACT",
    "OBJECT_CONSTRUCT", "OBJECT_KEYS", "OBJECT_AGG",
    "PARSE_JSON", "TRY_PARSE_JSON", "GET_PATH", "FLATTEN",
    "IFF", "IFNULL", "NVL", "NVL2", "NULLIF", "COALESCE", "ZEROIFNULL",
    "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR", "REGEXP_COUNT",
    "SPLIT", "SPLIT_PART", "STRTOK", "STRTOK_TO_ARRAY",
    "HASH", "MD5", "SHA1", "SHA2",
    "LISTAGG", "WITHIN GROUP", "QUALIFY",
    "RATIO_TO_REPORT", "CUME_DIST", "PERCENT_RANK", "NTILE",
    "CURRENT_DATABASE", "CURRENT_SCHEMA", "CURRENT_WAREHOUSE",
    "SYSTEM$TYPEOF", "TYPEOF", "IS_INTEGER", "IS_DECIMAL",
}

# Common SQL functions across all dialects
COMMON_FUNCTIONS: Set[str] = {
    "COUNT", "SUM", "AVG", "MIN", "MAX", "ABS", "ROUND", "CEIL", "FLOOR",
    "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "LENGTH", "SUBSTRING", "CONCAT",
    "REPLACE", "CAST", "CONVERT", "COALESCE", "NULLIF", "IFNULL",
    "NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
    "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
    "OVER", "PARTITION", "ORDER",
}


@dataclass
class ParsedSQL:
    """Structured representation of parsed SQL."""
    ast: Any
    dialect: str
    tables: List[str]
    columns: List[str]
    functions: List[str]
    aliases: dict
    raw_sql: str


class SQLParser:
    """
    Multi-dialect SQL parser using sqlglot.
    Supports: PostgreSQL, SQLite, DuckDB, BigQuery, Snowflake, ClickHouse, MySQL
    """

    def __init__(self, default_dialect: str = ""):
        self.default_dialect = self._normalize_dialect(default_dialect)

    def _normalize_dialect(self, dialect: str) -> Optional[str]:
        """Normalize dialect name to sqlglot format."""
        if not dialect:
            return None
        dialect_lower = dialect.lower().strip()
        return DIALECT_MAP.get(dialect_lower, dialect_lower)

    def parse(self, sql: str, dialect: str = "") -> ParsedSQL:
        """
        Parse SQL into a structured ParsedSQL object.
        
        Args:
            sql: The SQL query string
            dialect: SQL dialect (bigquery, snowflake, postgres, etc.)
        
        Returns:
            ParsedSQL object with AST and extracted metadata
        """
        normalized_dialect = self._normalize_dialect(dialect) or self.default_dialect
        
        try:
            if normalized_dialect:
                ast = sqlglot.parse_one(sql, read=normalized_dialect)
            else:
                ast = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError as e:
            # Try to parse with error recovery
            ast = self._parse_with_fallback(sql, normalized_dialect)
        
        identifiers = self._extract_all_identifiers(ast)
        
        return ParsedSQL(
            ast=ast,
            dialect=normalized_dialect or "auto",
            tables=identifiers.tables,
            columns=identifiers.columns,
            functions=identifiers.functions,
            aliases=identifiers.aliases,
            raw_sql=sql
        )

    def _parse_with_fallback(self, sql: str, dialect: Optional[str]) -> Any:
        """Try multiple dialects if primary fails."""
        fallback_dialects = ["postgres", "bigquery", "snowflake", None]
        
        for fallback in fallback_dialects:
            if fallback == dialect:
                continue
            try:
                if fallback:
                    return sqlglot.parse_one(sql, read=fallback)
                else:
                    return sqlglot.parse_one(sql)
            except:
                continue
        
        # Return a basic parse even if it has errors
        return sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)

    def extract_identifiers(self, sql: str, dialect: str = "") -> IdentifierSet:
        """
        Extract all identifiers (tables, columns, functions) from SQL.
        
        Args:
            sql: The SQL query string
            dialect: SQL dialect for proper parsing
        
        Returns:
            IdentifierSet with tables, columns, functions, and aliases
        """
        parsed = self.parse(sql, dialect)
        return IdentifierSet(
            tables=parsed.tables,
            columns=parsed.columns,
            functions=parsed.functions,
            aliases=parsed.aliases
        )

    def _extract_all_identifiers(self, ast: Any) -> IdentifierSet:
        """Extract all identifiers from AST."""
        tables = []
        columns = []
        functions = []
        aliases = {}

        # Extract tables (handle qualified names like schema.table)
        for table in ast.find_all(sqlglot.exp.Table):
            table_name = table.name
            if table.db:
                table_name = f"{table.db}.{table_name}"
            if table.catalog:
                table_name = f"{table.catalog}.{table_name}"
            tables.append(table_name)

        # Extract columns (handle qualified names like table.column)
        for col in ast.find_all(sqlglot.exp.Column):
            col_name = col.name
            if col.table:
                col_name = f"{col.table}.{col_name}"
            columns.append(col_name)

        # Extract functions
        for func in ast.find_all(sqlglot.exp.Func):
            func_name = func.__class__.__name__.upper()
            # Handle anonymous functions with actual names
            if hasattr(func, 'name') and func.name:
                func_name = func.name.upper()
            elif hasattr(func, 'sql_name'):
                func_name = func.sql_name().upper()
            functions.append(func_name)

        # Extract aliases
        for alias in ast.find_all(sqlglot.exp.Alias):
            alias_name = alias.alias
            if hasattr(alias.this, 'name'):
                aliases[alias_name] = alias.this.name
            else:
                aliases[alias_name] = str(alias.this)

        return IdentifierSet(
            tables=list(set(tables)),
            columns=list(set(columns)),
            functions=list(set(functions)),
            aliases=aliases
        )

    def get_dialect_functions(self, dialect: str) -> Set[str]:
        """Get valid functions for a specific dialect."""
        normalized = self._normalize_dialect(dialect)
        
        functions = COMMON_FUNCTIONS.copy()
        
        if normalized == "bigquery":
            functions.update(BIGQUERY_FUNCTIONS)
        elif normalized == "snowflake":
            functions.update(SNOWFLAKE_FUNCTIONS)
        
        return functions

    def is_valid_function(self, func_name: str, dialect: str) -> bool:
        """Check if a function is valid for the given dialect."""
        valid_functions = self.get_dialect_functions(dialect)
        return func_name.upper() in valid_functions

    def transpile(self, sql: str, from_dialect: str, to_dialect: str) -> str:
        """
        Transpile SQL from one dialect to another.
        
        Args:
            sql: Source SQL query
            from_dialect: Source dialect
            to_dialect: Target dialect
        
        Returns:
            Transpiled SQL string
        """
        from_normalized = self._normalize_dialect(from_dialect)
        to_normalized = self._normalize_dialect(to_dialect)
        
        return sqlglot.transpile(
            sql,
            read=from_normalized,
            write=to_normalized,
            pretty=True
        )[0]

    def validate_syntax(self, sql: str, dialect: str = "") -> tuple[bool, Optional[str]]:
        """
        Validate SQL syntax for a given dialect.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            self.parse(sql, dialect)
            return True, None
        except sqlglot.errors.ParseError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
