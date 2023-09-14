from metaflow.decorators import StepDecorator

from metaflow.exception import MetaflowException

class DbtStepDecorator(StepDecorator):
    name = "dbt"

    defaults = {
        "config": None,
        "model": None
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(DbtStepDecorator, self).__init__(attributes, statically_defined)


    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        if self.attributes["config"] is None:
            raise MetaflowException("You must specify a configuration file for the target of the DBT commands")

        if self.attributes["model"] is None:
            raise MetaflowException("You must specify a model to run with DBT run")

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        print("running before the task code")
        print(f"config is: {self.attributes['config']}")
        print(f"model is: {self.attributes['model']}")
