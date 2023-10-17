from metaflow import step, FlowSpec, secrets, conda, environment
from config import SECRET_SRC, HOST_ENV
import os


class DBTCreateDBFlow(FlowSpec):
    @conda(libraries={"psycopg2": "2.9.6"})
    @secrets(sources=SECRET_SRC)
    @environment(vars=HOST_ENV)
    @step
    def start(self):
        dbhost = os.environ.get("dbhost")
        user = os.environ.get("username")
        password = os.environ.get("password")
        print(f"Try creating Databases on host: {dbhost}")

        from psycopg2 import connect, sql
        from psycopg2.errors import DuplicateDatabase

        def try_create_db(cursor, dbname):
            try:
                query = "CREATE DATABASE {};"
                cursor.execute(sql.SQL(query).format(sql.Identifier(dbname)))
            except DuplicateDatabase:
                print(f"database {dbname} already exists.")
                pass

        connection = connect(f"host={dbhost} user={user} password={password}")
        connection.autocommit = True

        try:
            cursor = connection.cursor()
            try_create_db(cursor, "dbt_decorator")
        finally:
            connection.close()

        self.next(self.end)

    @step
    def end(self):
        print("Done! 🏁")


if __name__ == "__main__":
    DBTCreateDBFlow()
