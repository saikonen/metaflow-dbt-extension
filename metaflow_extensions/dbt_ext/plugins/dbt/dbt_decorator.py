import os
import sys
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

from .dbt_executor import DBTExecutor, DBTProjectConfig


class CommandNotSupported(MetaflowException):
    headline = "DBT command not supported"


class MissingProfiles(MetaflowException):
    headline = "Missing DBT Profiles configuration"


class DbtStepDecorator(StepDecorator):
    """
    Decorator to execute DBT models before a step execution begins.


    Parameters
    ----------
    command: str, optional. Default 'run'
        DBT command to execute. Default is 'run'.
        Supported commands are: run, seed
    project_dir: str, optional
        Path to the DBT project that contains a 'dbt_project.yml'.
        If not specified, the current folder and parent folders will be tried.
    models: List[str], optional
        List of model name(s) to run. All models will be run by default if no model name is provided.
    target: str, optional
        Chooses which target to load from the profiles.yml file.
        If not specified, it will use the default target from the profiles.
    profiles: Dict[str, Union[str, Dict]]
        a configuration dictionary that will be translated into a valid profiles.yml for the dbt CLI.
    """

    name = "dbt"
    allow_multiple = True

    defaults = {
        "command": "run",
        "project_dir": None,
        "models": None,
        "target": None,
        "profiles": None,
        "generate_docs": False,
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(DbtStepDecorator, self).__init__(attributes, statically_defined)

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        if self.attributes["profiles"] is None and not os.path.exists("./profiles.yml"):
            raise MissingProfiles(
                "You must provide profiles configuration for the DBT decorator.\n"
                "Either provide a dictionary for the 'profiles=' attribute "
                "or create a 'profiles.yml' file in the flow folder."
            )

        cmd = self.attributes["command"]

        if cmd not in ["run", "seed"]:
            raise CommandNotSupported(f"command '{cmd}' is not supported.")

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
        # Fix for conda environments not being able to locate the dbt binary due to conda decorator extending PATH too late in the lifecycle.
        python_loc = os.path.dirname(os.path.realpath(sys.executable))
        original_path = os.environ.get("PATH")
        if python_loc not in original_path:
            os.environ["PATH"] = os.pathsep.join([python_loc, original_path])

        executor = DBTExecutor(
            models=self.attributes["models"],
            project_dir=self.attributes["project_dir"],
            target=self.attributes["target"],
            profiles=self.attributes["profiles"],
        )

        cmd = self.attributes["command"]
        if cmd == "run":
            out = executor.run()
            print(out)
        if cmd == "seed":
            out = executor.seed()
            print(out)

        if self.attributes["generate_docs"]:
            out = executor.generate_docs()
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
                "static_index": executor.static_index,
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
        config = DBTProjectConfig(self.attributes["project_dir"])
        paths = config.project_file_paths()

        # TODO: verify keys for possible collisions.
        files = [(path, path) for path in paths]
        return files
