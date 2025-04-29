import pandas as pd
import clickhouse_connect
import os
from dotenv import load_dotenv # Import load_dotenv
from typing import List, Optional

# Load environment variables from a .env file if it exists
load_dotenv()

class DataBase:
    """
    A class to interact with a ClickHouse database.

    Provides methods to execute SQL queries and perform upserts using pandas DataFrames.
    Connection details are read from environment variables by default.
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
