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

If you only want to run a specific model as part of a step, you can specify this with the `model=` attribute
```python
@dbt(model="customers")
```

## Configuration options

### Project directory
You might want to keep the DBT project separately nested within the Flow project. In these cases you would need to specify the location of the DBT project folder, due to the way project lookup works.
This can be done by specifying the project location as a relative or absolute path within the decorator
```python
@dbt(model="customers", project_dir="./dbt_project")
```