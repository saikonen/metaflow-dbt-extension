# DBT extension for Metaflow

This extension adds support for executing DBT models as part of steps in Metaflow Flows via decorators.

## Basic usage

Having a `dbt_project.yml` as part of your Flow project will allow executing `dbt run` as a pre-step to any task by simply adding the decorator to a step.

```python
@dbt
@step
def start(self):
    # DBT Models have been run when step execution starts

    self.next(self.second_step)
```

If you only want to run a specific model as part of a step, you can specify this with the `models=` attribute
```python
@dbt(models="customers")
```

## Configuration options

### Project directory
You might want to keep the DBT project separately nested within the Flow project. In these cases you would need to specify the location of the DBT project folder, due to the way project lookup works.
This can be done by specifying the project location as a relative or absolute path within the decorator
```python
@dbt(models="customers", project_dir="./dbt_project")
```

### Supplying credentials

When deploying a DBT flow to be executed remotely, we do not want to bundle up sensitive credentials into the code package. Therefore a plain text `profiles.yml` will not suffice.
We can utilize the environment variable replacement that DBT offers to get around this.

example profiles:
```python
dbt_decorator:
  outputs:
    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: "{{ env_var('DBT_POSTGRES_USER') }}"
      pass: "{{ env_var('DBT_POSTGRES_PW') }}"
      dbname: dbt_decorator
      schema: dev_jaffle_schema

    prod:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: "{{ env_var('DBT_POSTGRES_USER') }}"
      pass: "{{ env_var('DBT_POSTGRES_PW') }}"
      dbname: dbt_decorator
      schema: prod_jaffle_schema

  target: dev
```

Note: any profiles.yml in the *flow project folder* will be packaged, so make sure that they do not contain sensitive secrets.

We can supply the environment variables in various ways, for example
- having them already present in the execution environment
- supplying them with the `@environment` decorator in the flow (this still ends up bundling secrets into the package, but is good for testing)
- hydrating environment variables with the `@secrets` decorator from a secret manager.

## Examples

Check out the example flows in the [`/examples`](./examples/README.md) folder for detailed usage.