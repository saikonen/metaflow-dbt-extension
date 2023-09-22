import glob
import os
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
    target: str, optional
        Chooses which target to load from the profiles.yml file.
        If not specified, it will use the default target from the profiles.
    """

    name = "dbt"

    defaults = {
        "project_dir": None,
        "model": None,
        "target": None,
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
        executor = DBTExecutor(
            model=self.attributes["model"],
            project_dir=self.attributes["project_dir"],
            target=self.attributes["target"],
        )

        out = executor.run()
        print(out)

        # Write DBT run artifacts as task artifacts.
        # TODO: If required, look into making this available *during* the task execution as well,
        # by somehow making f.ex. self.run_results be persisted before the task initializes.
        # As it is now, the run_results will only be available through self in subsequent steps,
        # but not the one with the decorator.
        def _dbt_artifacts_iterable():
            artifacts = {
                "run_results": executor.run_results,
                "semantic_manifest": executor.semantic_manifest,
                "manifest": executor.manifest,
                "sources": executor.sources,
                "catalog": executor.catalog,
            }
            for name, func in artifacts.items():
                val = func()
                if val is None:
                    continue
                yield (name, val)

        task_datastore.save_artifacts(_dbt_artifacts_iterable())

    def add_to_package(self):
        """
        Called to add custom packages needed for a decorator. This hook will be
        called in the `MetaflowPackage` class where metaflow compiles the code package
        tarball. This hook is invoked in the `MetaflowPackage`'s `path_tuples`
        function. The `path_tuples` function is a generator that yields a tuple of
        `(file_path, arcname)`.`file_path` is the path of the file in the local file system;
        the `arcname` is the path of the file in the constructed tarball or the path of the file
        after decompressing the tarball.

        Returns a list of tuples where each tuple represents (file_path, arcname)
        """
        executor = DBTExecutor(
            model=self.attributes["model"],
            project_dir=self.attributes["project_dir"],
            target=self.attributes["target"],
        )
        paths = executor.project_file_paths()

        # TODO: verify keys for possible collisions.
        files = [(path, path) for path in paths]
        return files
