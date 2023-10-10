import subprocess
import os
import json
import yaml
import glob
from typing import Dict, Optional

from metaflow.exception import MetaflowException
from metaflow.util import which


class DBTExecutionFailed(MetaflowException):
    headline = "DBT Run execution failed"


# TODO: There is a choice to utilize the Python library provided by DBT for performing the run, and accessing run results as well.
# This would introduce a heavy dependency for the decorator use case, which can be completely avoided with the custom implementation
# via calling the CLI via subprocess only at the point when execution needs to happen. Decide on the approach after PoC is complete.
class DBTExecutor:
    def __init__(self, model: str = None, project_dir: str = None, target: str = None):
        self.model = model
        self.project_dir = project_dir
        self.target = target
        self.bin = which("./dbt") or which("dbt")
        if self.bin is None:
            raise DBTExecutionFailed("Can not find DBT binary. Please install DBT")

        conf = DBTProjectConfig(project_dir)
        self._project_config = conf._project_config

    def run_results(self) -> Optional[Dict]:
        return self._read_dbt_artifact("run_results.json")

    def semantic_manifest(self) -> Optional[Dict]:
        return self._read_dbt_artifact("semantic_manifest.json")

    def manifest(self) -> Optional[Dict]:
        return self._read_dbt_artifact("manifest.json")

    def catalog(self) -> Optional[Dict]:
        return self._read_dbt_artifact("catalog.json")

    def sources(self) -> Optional[Dict]:
        return self._read_dbt_artifact("sources.json")

    def run(self) -> str:
        args = ["--fail-fast"]
        if self.project_dir is not None:
            args.extend(["--project-dir", self.project_dir])
        if self.model is not None:
            args.extend(["--model", self.model])
        if self.target is not None:
            args.extend(["--target", self.target])

        return self._call("run", args)

    def _read_dbt_artifact(self, name: str):
        artifact = os.path.join(
            ".",
            self.project_dir or "",
            self._project_config.get("target", "target"),
            name,
        )
        try:
            with open(artifact) as m:
                return json.load(m)
        except FileNotFoundError:
            return None

    def _call(self, cmd, args):
        try:
            return subprocess.check_output(
                [self.bin, cmd] + args, stderr=subprocess.PIPE
            ).decode()
        except subprocess.CalledProcessError as e:
            raise DBTExecutionFailed(msg=e.output.decode())


# We want a separate construct for the project config, so this can be parsed without requiring the dbt binary to be present on the system.
# This way users deploying to remote execution do not need to install DBT on their own machine.
class DBTProjectConfig:
    def __init__(self, project_dir: str = None):
        self.project_dir = project_dir

    @property
    def project_config(self):
        config_path = os.path.join(self.project_dir or "./", "dbt_project.yml")

        try:
            with open(config_path) as f:
                return yaml.load(f, Loader=yaml.Loader)
        except FileNotFoundError:
            raise MetaflowException("No configuration file 'dbt_project.yml' found")

    def project_file_paths(self):
        """
        Return a list of files required for the DBT project.
        Used to include necessary files in the codepackage
        """
        files = []
        _inc = [
            f
            for f in [
                "./profiles.yml",
                os.path.join(self.project_dir or "", "dbt_project.yml"),
            ]
            if os.path.exists(f)
        ]
        files.extend(_inc)

        for component in [
            "model-paths",
            "seed-paths",
            "test-paths",
            "analysis-paths",
            "macro-paths",
        ]:
            rel_path = self.project_config.get(component, [None])[
                0
            ]  # dbt profile config defines the file path inside a List.
            if rel_path is None:
                continue
            component_path = os.path.join(self.project_dir or "", rel_path)
            for path in glob.glob(os.path.join(component_path, "*"), recursive=True):
                files.append(path)

        return files
