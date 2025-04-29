"""Database interaction class for ClickHouse."""

import os
from typing import List, Dict, Any, Type, Optional
from datetime import datetime, date
import pandas as pd
import clickhouse_connect
import logging
from dataclasses import fields, is_dataclass
import typing # Import typing for get_origin, get_args

# Ensure these imports are available if needed for type mapping, though
# the current map uses built-in types and datetime/date
# from uuid import UUID
# from decimal import Decimal

# Mapping Python types to ClickHouse types
# Note: ClickHouse types can be Nullable(Type) for optional fields.
# clickhouse-connect handles mapping Python None to Nullable types automatically.
PYTHON_TO_CLICKHOUSE_TYPE_MAP = {
    str: "String",
    int: "Int64", # Using Int64 as a safe default
    float: "Float64",
    bool: "UInt8", # ClickHouse uses numeric types for boolean (0 or 1)
    datetime: "DateTime64(3)", # Using DateTime64 with millisecond precision
    date: "Date",
    # Add more mappings as needed (e.g., UUID, Decimal)
    # UUID: "UUID",
    # Decimal: "Decimal(18, 9)", # Example Decimal mapping
}

def _map_python_type_to_clickhouse(py_type: Type) -> str:
    """Maps a Python type (or Optional type) to a ClickHouse type string."""
    # Handle Optional types: get the inner type and make it Nullable
    origin = typing.get_origin(py_type)
    args = typing.get_args(py_type)

    if origin is typing.Union:
        # Check if it's an Optional (Union with NoneType)
        if len(args) == 2 and type(None) in args:
            # Find the actual type T
            actual_type = next((arg for arg in args if arg is not type(None)), None)
            if actual_type:
                py_type = actual_type # Use the inner type for mapping
                # Now map the inner type and mark as nullable
                ch_type = PYTHON_TO_CLICKHOUSE_TYPE_MAP.get(py_type)
                if ch_type:
                    return f"Nullable({ch_type})"
                else:
                     raise TypeError(f"No ClickHouse mapping found for inner type {py_type.__name__} in Optional[{py_type.__name__}]")
            else:
                 raise TypeError(f"Could not determine inner type for Optional type {py_type}")
        else:
             # Handle other Union types if needed, for now raise error
             raise TypeError(f"Unsupported Union type structure: {py_type}")
    elif origin is not None:
         # Handle other generic types like List, Dict, etc.
         # For now, raise an error or map to String if appropriate
         raise TypeError(f"Unsupported generic type: {py_type}. Consider using String or specialized ClickHouse types.")


    # Handle non-Optional types
    ch_type = PYTHON_TO_CLICKHOUSE_TYPE_MAP.get(py_type)
    if ch_type:
        return ch_type
    else:
        raise TypeError(f"No ClickHouse mapping found for Python type {py_type.__name__}")


class DataBase:
    """
    A class to interact with a ClickHouse database using clickhouse-connect.

    Provides methods to execute SQL queries, perform upserts using pandas DataFrames,
    and create tables from dataclass definitions. Connection details are read from
    environment variables by default.

    Note: This class holds a single clickhouse-connect client instance. While
    session ID generation is disabled to mitigate session-related concurrency
    issues (as per clickhouse-connect documentation for shared clients),
    concurrent access from multiple threads/processes writing to the *same*
    client instance might still require external synchronization if not using
    bulk operations like upsert_dataframe. Reading via execute_query is generally
    safer for concurrent access. For heavy concurrent writes, consider a connection pool.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initializes the database connection. Reads credentials from environment
        variables if not provided.

        Environment variables used:
        - CLICKHOUSE_HOST (default: 'localhost')
        - CLICKHOUSE_PORT (default: 8123)
        - CLICKHOUSE_DATABASE (default: 'default')
        - CLICKHOUSE_USER (default: 'default')
        - CLICKHOUSE_PASSWORD (default: '')
        """
        self.logger = logging.getLogger(__name__)

        self.host = host or os.environ.get("CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.environ.get("CLICKHOUSE_PORT", 8123))
        self.database = database or os.environ.get("CLICKHOUSE_DATABASE", "default")
        self.user = user or os.environ.get("CLICKHOUSE_USER", "default")
        self.password = password or os.environ.get("CLICKHOUSE_PASSWORD", "")

        self.client: Optional[clickhouse_connect.ClickHouseClient] = None
        self._connect()

    def _connect(self):
        """Establishes the connection to the ClickHouse database."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.user,
                password=self.password,
                # Disable session ID generation for shared client instance
                # This argument is removed as it's not supported in newer clickhouse-connect versions
                # session_id_generator=None
            )
            # Test connection
            self.client.command('SELECT 1')
            self.logger.info(f"Connected to ClickHouse database '{self.database}' at {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {e}")
            self.client = None # Ensure client is None if connection fails
            raise # Re-raise the exception after logging

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a read query and returns the result as a pandas DataFrame.

        Args:
            query: The SQL query string.

        Returns:
            A pandas DataFrame containing the query results. Returns an empty
            DataFrame if the client is not connected or an error occurs.
        """
        if not self.client:
            self.logger.error("Cannot execute query: Database client is not connected.")
            return pd.DataFrame()
        try:
            self.logger.debug(f"Executing query: {query}")
            result = self.client.query(query)
            return result.result_df
        except Exception as e:
            self.logger.error(f"Error executing query: {e}\nQuery: {query}")
            return pd.DataFrame() # Return empty DataFrame on error

    def execute_command(self, command: str):
         """
         Executes a command (e.g., CREATE TABLE, INSERT, ALTER).

         Args:
             command: The SQL command string.
         """
         if not self.client:
             self.logger.error("Cannot execute command: Database client is not connected.")
             return
         try:
             self.logger.debug(f"Executing command: {command}")
             self.client.command(command)
             self.logger.debug("Command executed successfully.")
         except Exception as e:
             self.logger.error(f"Error executing command: {e}\nCommand: {command}")
             raise # Re-raise the exception

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, unique_cols: List[str]):
        """
        Performs an upsert operation using a pandas DataFrame.
        This method leverages ClickHouse's ReplacingMergeTree or similar engines
        by inserting data. ClickHouse handles the deduplication based on the
        engine's primary key or sorting key.

        Note: For ReplacingMergeTree, the 'unique_cols' should typically correspond
        to the table's ORDER BY key. For unique constraints, you might need
        a different engine or approach (e.g., Unique engine, though less common).
        This method assumes the table engine handles deduplication on insert.

        Args:
            df: The pandas DataFrame to upsert.
            table_name: The name of the target table.
            unique_cols: A list of column names that define uniqueness.
                         Used here primarily for logging/context, as the actual
                         deduplication logic is engine-dependent.
        """
        if not self.client:
            self.logger.error("Cannot upsert dataframe: Database client is not connected.")
            return
        if df.empty:
            self.logger.info(f"DataFrame for table '{table_name}' is empty. Skipping upsert.")
            return

        self.logger.info(f"Upserting {len(df)} rows into table '{table_name}' (unique on {unique_cols})...")
        try:
            # clickhouse-connect's insert handles DataFrame directly
            # It automatically maps pandas dtypes to ClickHouse types and handles None/NaN
            self.client.insert_df(table_name, df)
            self.logger.info(f"Successfully upserted {len(df)} rows into '{table_name}'.")
        except Exception as e:
            self.logger.error(f"Error upserting dataframe into '{table_name}': {e}")
            raise # Re-raise the exception

    def create_table(
        self,
        dataclass_type: Type,
        table_name: str,
        engine: str = 'MergeTree()',
        order_by: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None, # Often same as order_by for MergeTree
        if_not_exists: bool = True
    ):
        """
        Creates a ClickHouse table based on a dataclass definition.

        Args:
            dataclass_type: The Python dataclass type defining the table schema.
            table_name: The name of the table to create.
            engine: The ClickHouse table engine (default: 'MergeTree()').
            order_by: List of column names for the ORDER BY clause. Required for MergeTree.
            primary_key: List of column names for the PRIMARY KEY clause (optional, often same as ORDER BY).
            if_not_exists: If True, adds IF NOT EXISTS to the CREATE TABLE statement.
        """
        if not is_dataclass(dataclass_type):
            raise TypeError(f"Provided type {dataclass_type.__name__} is not a dataclass.")

        if 'MergeTree' in engine and not order_by:
             raise ValueError("ORDER BY clause is required for MergeTree engine.")

        columns = []
        for field in fields(dataclass_type):
            try:
                ch_type = _map_python_type_to_clickhouse(field.type)
                columns.append(f"`{field.name}` {ch_type}")
            except TypeError as e:
                self.logger.error(f"Could not map type for field '{field.name}' in dataclass '{dataclass_type.__name__}': {e}")
                raise # Stop if a type cannot be mapped

        columns_sql = ",\n    ".join(columns)

        order_by_sql = ""
        if order_by:
            order_by_sql = f"\nORDER BY ({', '.join([f'`{col}`' for col in order_by])})"

        primary_key_sql = ""
        if primary_key:
             primary_key_sql = f"\nPRIMARY KEY ({', '.join([f'`{col}`' for col in primary_key])})"
             # Note: PRIMARY KEY is usually a prefix of ORDER BY for MergeTree

        create_table_sql = f"""
        CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}`{table_name}` (
            {columns_sql}
        ) ENGINE = {engine}{order_by_sql}{primary_key_sql}
        """

        self.logger.info(f"Attempting to create table '{table_name}'...")
        try:
            self.execute_command(create_table_sql)
            self.logger.info(f"Table '{table_name}' created or already exists.")
        except Exception as e:
            self.logger.error(f"Failed to create table '{table_name}': {e}")
            raise # Re-raise the exception

    def close(self):
        """Closes the database connection."""
        if self.client:
            try:
                self.client.close()
                self.logger.info("ClickHouse connection closed.")
            except Exception as e:
                self.logger.error(f"Error closing ClickHouse connection: {e}")
            finally:
                self.client = None

    def __enter__(self):
        """Context manager entry point."""
        # Connection is established in __init__
        if not self.client:
             # Attempt to reconnect if not connected
             self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close()
