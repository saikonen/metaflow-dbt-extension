from metaflow import step, FlowSpec, dbt, environment, secrets
from config import DBT_PROFILES, SECRET_SRC, HOST_ENV


class DBTFlow(FlowSpec):
    @step
    def start(self):
        print("Start step for debugging")
        self.next(self.dbt_project, self.jaffle_seed)

    @dbt(project_dir="dbt_project", target="dev", profiles=DBT_PROFILES)
    @secrets(sources=SECRET_SRC)
    @environment(vars=HOST_ENV)
    @step
    def dbt_project(self):
        print("dbt_project DBT run")
        self.next(self.join)

    @dbt(command="seed", project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @secrets(sources=SECRET_SRC)
    @environment(vars=HOST_ENV)
    @step
    def jaffle_seed(self):
        # jaffle_shop example needs to be seeded before 'dbt run' works for its models.
        print("Seeded jaffle_shop")
        self.next(self.jaffle_staging)

    @dbt(models=["staging"], project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @secrets(sources=SECRET_SRC)
    @environment(vars=HOST_ENV)
    @step
    def jaffle_staging(self):
        print("jaffle_shop DBT run: staging")
        self.next(self.jaffle_customers, self.jaffle_orders)

    @dbt(models=["customers"], project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @secrets(sources=SECRET_SRC)
    @environment(vars=HOST_ENV)
    @step
    def jaffle_customers(self):
        print("jaffle_shop DBT run: customers")
        self.next(self.jaffle_join)

    @dbt(models=["orders"], project_dir="./jaffle_shop", profiles=DBT_PROFILES)
    @secrets(sources=SECRET_SRC)
    @environment(vars=HOST_ENV)
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
