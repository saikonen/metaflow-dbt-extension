from metaflow.decorators import StepDecorator

class DbtStepDecorator(StepDecorator):
    name = "dbt"

    defaults = {
        "config": None,
        "model": None
    }
