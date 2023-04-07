import os
import sqlite3
import traceback

import pandas as pd
from snowflake.connector import connect
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def import_sqlite_to_snowflake(snowflake_credentials, base_directory):
    # Connect to Snowflake
    engine = create_engine(URL(**snowflake_credentials))
    session = sessionmaker(bind=engine)()

    # Loop through all directories in the base_directory
    for folder in os.listdir(base_directory):
        folder_path = os.path.join(base_directory, folder)

        # Check if the path is a directory and contains a .sqlite file
        if os.path.isdir(folder_path) and f"{folder}.sqlite" in os.listdir(folder_path):
            sqlite_file_path = os.path.join(folder_path, f"{folder}.sqlite")

            # Connect to the SQLite database
            sqlite_conn = sqlite3.connect(sqlite_file_path)
            sqlite_conn.text_factory = lambda b: b.decode(errors='ignore')

            try:
                # Drop schema in Snowflake if it exists
                session.execute(f"DROP SCHEMA IF EXISTS {folder.lower()};")
                session.commit()

                # Create schema in Snowflake
                session.execute(f"CREATE SCHEMA {folder.lower()};")
                session.commit()

                # Get the list of tables in the SQLite database
                tables = sqlite_conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

                # Import each table to Snowflake
                for table_name, in tables:
                    df = pd.read_sql_query(f"SELECT * FROM {table_name};", sqlite_conn)

                    # There's an issue to import data from wta_1 database, so we need to do some data cleaning
                    if folder == 'wta_1':
                        # for any columns, if it is a number (int or float), convert the value to '' if it is None
                        for col in df.columns:
                            if df[col].dtype in ['int64', 'float64']:
                                df[col] = df[col].apply(lambda x: None if x == '' else x)

                        columns_need_to_be_converted = ['birth_date', 'ranking_points', 'tours', 'ranking_date', 'ranking', 'player_id']
                        for col in columns_need_to_be_converted:
                            if col in df.columns:
                                df[col] = df[col].apply(lambda x: None if x == '' else x)

                    # Rename columns to lower case
                    df.rename(columns={col: col.lower() for col in df.columns}, inplace=True)

                    df.to_sql(
                        table_name.lower(),
                        engine,
                        schema=folder.lower(),
                        if_exists="replace",
                        index=False,
                        method="multi",
                        chunksize=10000
                    )

                print(f"Successfully imported {folder}")

            except Exception as e:
                traceback.print_exc()
                print(f"Error importing {folder}: {e}")
                session.execute(f"DROP SCHEMA IF EXISTS {folder.lower()};")
                session.commit()

            finally:
                # Close SQLite connection
                sqlite_conn.close()

    # Close Snowflake connection
    session.close()



if __name__ == "__main__":
    # Set your Snowflake credentials here
    snowflake_credentials = {
        "user": "...",
        "password": "...",
        "account": "...",
        "warehouse": "...",
        "database": "...",
        "role": "..."
    }

    # Set your base directory containing the SQLite databases
    base_directory = "path/to/spider/database"

    # Run the import process
    import_sqlite_to_snowflake(snowflake_credentials, base_directory)