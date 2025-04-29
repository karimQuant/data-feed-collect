import pandas as pd
import clickhouse_connect
from typing import List, Optional

class DataBase:
    """
    A class to interact with a ClickHouse database.

    Provides methods to execute SQL queries and perform upserts using pandas DataFrames.
    """
    def __init__(self, host: str, port: int, database: str, user: str, password: Optional[str] = None):
        """
        Initializes the DataBase connection.

        Args:
            host: The database host address.
            port: The database port.
            database: The database name.
            user: The database user.
            password: The database password (optional).
        """
        self._client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database,
            username=user,
            password=password
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
