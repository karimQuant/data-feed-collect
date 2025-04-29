import pandas as pd
import clickhouse_connect
import os
import dataclasses # Import dataclasses
import typing # Import typing for get_origin, get_args
from typing import List, Optional, Type # Keep specific imports
from datetime import datetime, date # Import datetime and date for type mapping
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
load_dotenv()

# Basic mapping from Python types to ClickHouse types
# Note: This is a basic mapping and might need expansion for more complex types
PYTHON_TO_CLICKHOUSE_TYPE_MAP = {
    str: "String",
    int: "Int64", # Using Int64 as a safe default
    float: "Float64",
    bool: "UInt8", # ClickHouse uses numeric types for boolean
    datetime: "DateTime64(3)", # Using DateTime64 with millisecond precision
    date: "Date",
    # Add more mappings as needed (e.g., UUID, Decimal)
}

def _map_python_type_to_clickhouse(py_type: Type) -> str:
    """Maps a Python type to a ClickHouse type string, handling Optional."""
    is_nullable = False
    origin = typing.get_origin(py_type)
    args = typing.get_args(py_type)

    if origin is typing.Union:
        # Check if it's an Optional (Union with NoneType)
        if len(args) == 2 and type(None) in args:
            # Find the actual type T
            actual_type = args[0] if args[1] is type(None) else args[1]
            py_type = actual_type
            is_nullable = True
        else:
             # Handle other Union types if needed, for now raise error
             raise TypeError(f"Unsupported Union type structure: {py_type}")
    elif origin is not None:
         # Handle other generic types like List, Dict, etc.
         raise TypeError(f"Unsupported generic type: {py_type}. Consider using String or specialized ClickHouse types.")


    ch_type = PYTHON_TO_CLICKHOUSE_TYPE_MAP.get(py_type)

    if ch_type is None:
        raise TypeError(f"No direct ClickHouse mapping found for Python type: {py_type}")

    return f"Nullable({ch_type})" if is_nullable else ch_type


class DataBase:
    """
    A class to interact with a ClickHouse database.

    Provides methods to execute SQL queries, perform upserts using pandas DataFrames,
    and create tables from dataclass definitions. Connection details are read from
    environment variables by default.
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
        Initializes the DataBase connection.

        Connection details are read from arguments first, then from environment
        variables (CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE,
        CLICKHOUSE_USER, CLICKHOUSE_PASSWORD).

        Args:
            host: The database host address. Defaults to env var CLICKHOUSE_HOST.
            port: The database port. Defaults to env var CLICKHOUSE_PORT.
            database: The database name. Defaults to env var CLICKHOUSE_DATABASE.
            user: The database user. Defaults to env var CLICKHOUSE_USER.
            password: The database password (optional). Defaults to env var CLICKHOUSE_PASSWORD.
        """
        # Use provided arguments or fall back to environment variables
        db_host = host if host is not None else os.getenv("CLICKHOUSE_HOST")
        db_port_str = str(port) if port is not None else os.getenv("CLICKHOUSE_PORT") # Get as string first
        db_database = database if database is not None else os.getenv("CLICKHOUSE_DATABASE")
        db_user = user if user is not None else os.getenv("CLICKHOUSE_USER")
        db_password = password if password is not None else os.getenv("CLICKHOUSE_PASSWORD")

        # Convert port to int, handling potential errors
        db_port = None
        if db_port_str:
            try:
                db_port = int(db_port_str)
            except ValueError:
                print(f"Warning: Invalid value for CLICKHOUSE_PORT environment variable: {db_port_str}. Must be an integer.")
                # Keep db_port as None, will raise error below if required

        # Ensure required parameters are available
        if not db_host or not db_port or not db_database or not db_user:
             missing = []
             if not db_host: missing.append("host (or CLICKHOUSE_HOST)")
             if not db_port: missing.append("port (or CLICKHOUSE_PORT)")
             if not db_database: missing.append("database (or CLICKHOUSE_DATABASE)")
             if not db_user: missing.append("user (or CLICKHOUSE_USER)")
             raise ValueError(f"Missing required database connection parameters: {', '.join(missing)}")


        self._client = clickhouse_connect.get_client(
            host=db_host,
            port=db_port,
            database=db_database,
            username=db_user,
            password=db_password
        )

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query and returns the result as a pandas DataFrame.

        Args:
            query: The SQL query string to execute.

        Returns:
            A pandas DataFrame containing the query results.
        """
        try:
            df = self._client.query_df(query)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            raise # Re-raise the exception after printing

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, unique_cols: List[str]):
        """
        Inserts or updates data from a pandas DataFrame into a ClickHouse table.

        Note: ClickHouse does not have a standard 'UPSERT' statement like other
        databases. This method performs an efficient insert using clickhouse-connect.
        For true upsert behavior based on `unique_cols`, the target table must
        be configured appropriately (e.g., using a ReplacingMergeTree engine
        with a suitable primary key and/or version column, or requiring manual
        mutation logic outside this method).

        This method primarily uses `client.insert_df`, which is highly efficient
        for bulk inserts. If the table is a ReplacingMergeTree and `unique_cols`
        correspond to the primary key, inserting new data with higher version
        will replace older rows with the same primary key.

        Args:
            df: The pandas DataFrame containing the data to upsert.
            table_name: The name of the target table in ClickHouse.
            unique_cols: A list of column names that define uniqueness.
                         (Used for documentation/intent; actual upsert logic
                         depends on ClickHouse table engine configuration).
        """
        if not unique_cols:
             print("Warning: unique_cols list is empty. This operation will perform a simple insert.")

        try:
            # clickhouse-connect's insert_df is the most efficient way to load data
            # The actual upsert behavior depends on the table engine (e.g., ReplacingMergeTree)
            # and how unique_cols relate to the primary key and version column.
            self._client.insert_df(table_name, df)
            print(f"Successfully inserted/upserted {len(df)} rows into table '{table_name}'.")
        except Exception as e:
            print(f"Error inserting/upserting data into table '{table_name}': {e}")
            raise # Re-raise the exception after printing

    def create_table(
        self,
        dataclass_type: Type,
        table_name: str,
        engine: str = 'MergeTree()',
        order_by: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        if_not_exists: bool = True
    ):
        """
        Creates a ClickHouse table based on a dataclass definition.

        Args:
            dataclass_type: The Python dataclass type to use for the schema.
            table_name: The name of the table to create in ClickHouse.
            engine: The ClickHouse table engine (e.g., 'MergeTree()', 'ReplacingMergeTree(version_column)').
                    Defaults to 'MergeTree()'.
            order_by: A list of column names for the ORDER BY clause. Required for MergeTree family engines.
            primary_key: A list of column names for the PRIMARY KEY clause (optional).
            if_not_exists: If True, adds 'IF NOT EXISTS' to the CREATE TABLE statement.
        """
        if not dataclasses.is_dataclass(dataclass_type):
            raise TypeError(f"Provided type {dataclass_type} is not a dataclass.")

        # Check for required ORDER BY for MergeTree family engines
        if 'MergeTree' in engine and not order_by:
             raise ValueError(f"ORDER BY clause is required for MergeTree family engines like '{engine}'. Please provide 'order_by'.")

        columns = []
        for field in dataclasses.fields(dataclass_type):
            try:
                ch_type = _map_python_type_to_clickhouse(field.type)
                columns.append(f"`{field.name}` {ch_type}")
            except TypeError as e:
                # Re-raise with more context
                raise TypeError(f"Unsupported type for field '{field.name}' in dataclass '{dataclass_type.__name__}': {e}")


        columns_sql = ",\n    ".join(columns)

        create_sql = f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}`{table_name}` (\n    {columns_sql}\n)"
        create_sql += f" ENGINE = {engine}"

        if order_by:
            order_by_sql = ", ".join([f"`{col}`" for col in order_by])
            create_sql += f"\nORDER BY ({order_by_sql})"

        if primary_key:
            primary_key_sql = ", ".join([f"`{col}`" for col in primary_key])
            create_sql += f"\nPRIMARY KEY ({primary_key_sql})"

        print(f"Attempting to create table '{table_name}'...")
        # print(f"Executing CREATE TABLE query:\n{create_sql}") # Uncomment for debugging

        try:
            self._client.execute(create_sql)
            print(f"Table '{table_name}' created successfully (or already exists).")
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
            raise # Re-raise the exception


    def close(self):
        """Closes the database connection."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
