from .models import HallucinationReport, ValidationResult
from .sql_parser import SQLParser
from typing import Any, List, Optional


class HallucinationDetector:
    """
    Detects phantom (hallucinated) identifiers in SQL queries.
    
    Identifies:
    - Phantom tables: Tables referenced that don't exist in schema
    - Phantom columns: Columns referenced that don't exist in any table
    - Phantom functions: Functions that aren't valid for the dialect
    """

    def __init__(self):
        self.parser = SQLParser()

    def detect(self, sql: str, schema: Any, dialect: str = "") -> HallucinationReport:
        """
        Detect hallucinated identifiers in SQL query.
        
        Args:
            sql: The SQL query to analyze
            schema: SchemaSnapshot with tables/columns info
            dialect: SQL dialect (bigquery, snowflake, postgres, etc.)
        
        Returns:
            HallucinationReport with phantom identifiers and score
        """
        identifiers = self.parser.extract_identifiers(sql, dialect)
        
        # Detect phantom tables
        phantom_tables = self._detect_phantom_tables(identifiers.tables, schema)
        
        # Detect phantom columns
        phantom_columns = self._detect_phantom_columns(
            identifiers.columns, 
            identifiers.tables,
            identifiers.aliases,
            schema
        )
        
        # Detect phantom functions (dialect-aware)
        phantom_functions = self._detect_phantom_functions(
            identifiers.functions, 
            dialect,
            schema
        )
        
        # Calculate hallucination score
        total_identifiers = (
            len(identifiers.tables) + 
            len(identifiers.columns) + 
            len(identifiers.functions)
        )
        total_phantoms = (
            len(phantom_tables) + 
            len(phantom_columns) + 
            len(phantom_functions)
        )
        score = total_phantoms / max(1, total_identifiers)
        
        return HallucinationReport(
            phantom_tables=phantom_tables,
            phantom_columns=phantom_columns,
            phantom_functions=phantom_functions,
            hallucination_score=score
        )

    def _detect_phantom_tables(self, tables: List[str], schema: Any) -> List[str]:
        """Find tables that don't exist in the schema."""
        phantom = []
        for table in tables:
            # Handle qualified names (schema.table or catalog.schema.table)
            table_name = table.split(".")[-1]  # Get just the table name
            if not schema.has_table(table_name) and not schema.has_table(table):
                phantom.append(table)
        return phantom

    def _detect_phantom_columns(
        self, 
        columns: List[str], 
        tables: List[str],
        aliases: dict,
        schema: Any
    ) -> List[str]:
        """Find columns that don't exist in any referenced table."""
        phantom = []
        
        # Build a set of all valid columns from referenced tables
        valid_columns = set()
        for table in tables:
            table_name = table.split(".")[-1]
            table_info = schema.tables.get(table_name) or schema.tables.get(table)
            if table_info:
                for col in table_info.columns:
                    valid_columns.add(col.name.lower())
                    valid_columns.add(f"{table_name.lower()}.{col.name.lower()}")
        
        # Also add columns from aliased tables
        for alias, actual in aliases.items():
            actual_table = schema.tables.get(actual)
            if actual_table:
                for col in actual_table.columns:
                    valid_columns.add(f"{alias.lower()}.{col.name.lower()}")
        
        # Check each referenced column
        for col in columns:
            col_lower = col.lower()
            col_name_only = col.split(".")[-1].lower()
            
            # Check if column exists (with or without table qualifier)
            if col_lower not in valid_columns and col_name_only not in valid_columns:
                # Also check across all tables if no table qualifier
                if "." not in col:
                    found_anywhere = False
                    for table_info in schema.tables.values():
                        if col_name_only in [c.name.lower() for c in table_info.columns]:
                            found_anywhere = True
                            break
                    if not found_anywhere:
                        phantom.append(col)
                else:
                    phantom.append(col)
        
        return phantom

    def _detect_phantom_functions(
        self, 
        functions: List[str], 
        dialect: str,
        schema: Any
    ) -> List[str]:
        """Find functions that aren't valid for the dialect."""
        phantom = []
        
        # Get valid functions for this dialect
        valid_functions = self.parser.get_dialect_functions(dialect)
        
        # Also include any custom functions defined in schema
        if hasattr(schema, 'functions'):
            valid_functions.update(f.upper() for f in schema.functions)
        
        for func in functions:
            func_upper = func.upper()
            if func_upper not in valid_functions:
                # Some functions have different names in AST vs SQL
                # Check common aliases
                if not self._is_function_alias(func_upper, valid_functions):
                    phantom.append(func)
        
        return phantom

    def _is_function_alias(self, func: str, valid_functions: set) -> bool:
        """Check if function is a known alias of a valid function."""
        # Common AST to SQL function name mappings
        aliases = {
            "ANONYMOUS": True,  # Generic function wrapper
            "IF": "IFF",
            "SUBSTR": "SUBSTRING",
            "LEN": "LENGTH",
            "CHARINDEX": "POSITION",
        }
        
        if func in aliases:
            if aliases[func] is True:
                return True
            return aliases[func] in valid_functions
        
        return False

    def validate(self, sql: str, schema: Any, dialect: str = "") -> ValidationResult:
        """
        Validate SQL and return a ValidationResult.
        Convenience method that wraps detect().
        """
        report = self.detect(sql, schema, dialect)
        
        errors = []
        warnings = []
        
        for table in report.phantom_tables:
            errors.append(f"Table '{table}' does not exist in schema")
        
        for col in report.phantom_columns:
            errors.append(f"Column '{col}' does not exist in any table")
        
        for func in report.phantom_functions:
            warnings.append(f"Function '{func}' may not be valid for dialect '{dialect}'")
        
        return ValidationResult(
            is_valid=(len(errors) == 0),
            errors=errors,
            warnings=warnings,
            hallucination_report=report
        )
