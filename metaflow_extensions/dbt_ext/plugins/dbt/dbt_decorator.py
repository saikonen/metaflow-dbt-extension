from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

from .dbt_executor import DBTExecutor

class DbtStepDecorator(StepDecorator):
    """
    Decorator to execute DBT models before a step execution begins.


    Parameters
    ----------
    project_dir: str, optional
        Path to the DBT project that contains a 'dbt_project.yml'.
        If not specified, the current folder and parent folders will be tried.
    model: str, optional
        Name of model(s) to run. More than one model can be supplied separated by spaces.
        All models will be run by default if no model name is provided.
    """
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
        pass

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

        executor = DBTExecutor(
            model=self.attributes["model"],
            project_dir=self.attributes["project_dir"]
        )

        out = executor.run()
        print(out)
