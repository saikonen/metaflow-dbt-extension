from metaflow import step, FlowSpec, dbt, environment


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


class DBTFlow(FlowSpec):
    @step
    def start(self):
        print("Start step for debugging")
        self.next(self.dbt_project, self.jaffle_seed)

    @environment(vars={"username": "postgres", "password": "postgres"})
    @dbt(project_dir="./dbt_project", target="dev", profiles=DBT_PROFILES)
    @step
    def dbt_project(self):
        print("dbt_project DBT run")
        self.next(self.join)

    @environment(vars={"username": "postgres", "password": "postgres"})
    @dbt(command="seed", project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @step
    def jaffle_seed(self):
        # jaffle_shop example needs to be seeded before 'dbt run' works for its models.
        print("Seeded jaffle_shop")
        self.next(self.jaffle_staging)

    @environment(vars={"username": "postgres", "password": "postgres"})
    @dbt(models=["staging"], project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @step
    def jaffle_staging(self):
        print("jaffle_shop DBT run: staging")
        self.next(self.jaffle_customers, self.jaffle_orders)

    @environment(vars={"username": "postgres", "password": "postgres"})
    @dbt(models=["customers"], project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @step
    def jaffle_customers(self):
        print("jaffle_shop DBT run: customers")
        self.next(self.jaffle_join)

    @environment(vars={"username": "postgres", "password": "postgres"})
    @dbt(models=["orders"], project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @step
    def jaffle_orders(self):
        print("jaffle_shop DBT run: orders")
        print(f"RESULTS: {self.run_results['args']['invocation_command']}")
        self.next(self.jaffle_join)

    @step
    def jaffle_join(self, inputs):
        self.next(self.join)

    @step
    def join(self, inputs):
        print("join DBT runs")
        self.next(self.end)

    @step
    def end(self):
        print("Done! 🏁")


if __name__ == "__main__":
    DBTFlow()
