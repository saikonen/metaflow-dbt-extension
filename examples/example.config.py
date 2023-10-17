SECRET_SRC = ["replace-with-secret-source"]

HOST_ENV = {"dbhost": "replace-with-db-url"}

DBT_PROFILES = {
    "jaffle_shop": {
        "outputs": {
            "dev": {
                "type": "postgres",
                "threads": 1,
                "host": "{{ env_var('dbhost', 'localhost') }}",
                "port": 5432,
                "user": "{{ env_var('username') }}",
                "pass": "{{ env_var('password') }}",
                "dbname": "dbt_decorator",
                "schema": "dev_jaffle_schema",
            },
            "prod": {
                "type": "postgres",
                "threads": 1,
                "host": "{{ env_var('dbhost', 'localhost') }}",
                "port": 5432,
                "user": "{{ env_var('username') }}",
                "pass": "{{ env_var('password') }}",
                "dbname": "dbt_decorator",
                "schema": "prod_jaffle_schema",
            },
        },
        "target": "dev",
    },
    "dbt_project": {
        "outputs": {
            "dev": {
                "type": "postgres",
                "threads": 1,
                "host": "{{ env_var('dbhost', 'localhost') }}",
                "port": 5432,
                "user": "{{ env_var('username') }}",
                "pass": "{{ env_var('password') }}",
                "dbname": "dbt_decorator",
                "schema": "dev_dbt_schema",
            },
            "prod": {
                "type": "postgres",
                "threads": 1,
                "host": "{{ env_var('dbhost', 'localhost') }}",
                "port": 5432,
                "user": "{{ env_var('username') }}",
                "pass": "{{ env_var('password') }}",
                "dbname": "dbt_decorator",
                "schema": "prod_dbt_schema",
            },
        },
        "target": "dev",
    },
}
