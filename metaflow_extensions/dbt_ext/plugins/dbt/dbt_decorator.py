from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

from .dbt_executor import DBTExecutor

class DbtStepDecorator(StepDecorator):
    name = "dbt"

    defaults = {
        "project_dir": None,
        "model": None
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(DbtStepDecorator, self).__init__(attributes, statically_defined)


    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
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
        # print("running before the task code")
        # print(f"project_dir is: {self.attributes['project_dir']}")
        # print(f"model is: {self.attributes['model']}")

        executor = DBTExecutor(project_dir=self.attributes["project_dir"])

        out = executor.execute()
        print(out)
