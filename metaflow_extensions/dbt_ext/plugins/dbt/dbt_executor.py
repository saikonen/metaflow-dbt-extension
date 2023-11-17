import subprocess
import os
import tempfile
import json
import yaml
import glob
import shutil
from typing import Dict, List, Optional

from metaflow.exception import MetaflowException
from metaflow.util import which
from metaflow.plugins.datatools.s3 import S3


class DBTExecutionFailed(MetaflowException):
    headline = "DBT Run execution failed"


# TODO: There is a choice to utilize the Python library provided by DBT for performing the run, and accessing run results as well.
# This would introduce a heavy dependency for the decorator use case, which can be completely avoided with the custom implementation
# via calling the CLI via subprocess only at the point when execution needs to happen. Decide on the approach after PoC is complete.
class DBTExecutor:
    def __init__(
        self,
        models: List[str] = None,
        project_dir: str = None,
        target: str = None,
        profiles: Dict = {},
        state_store: str = None,
    ):
        self.models = " ".join(models) if models is not None else None
        self.project_dir = project_dir
        self.target = target
        self.bin = which("./dbt") or which("dbt")
        if self.bin is None:
            raise DBTExecutionFailed("Can not find DBT binary. Please install DBT")

        self.profiles = profiles
        conf = DBTProjectConfig(project_dir)
        self._project_config = conf.project_config
        self.state_store = state_store

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

    def static_index(self) -> Optional[Dict]:
        return self._read_dbt_artifact("static_index.html", raw=True)

    def run(self) -> str:
        args = ["--fail-fast"]
        if self.project_dir is not None:
            args.extend(["--project-dir", self.project_dir])
        if self.models is not None:
            args.extend(["--models", self.models])
        if self.target is not None:
            args.extend(["--target", self.target])

        return self._call("run", args)

    def seed(self) -> str:
        args = []
        if self.project_dir is not None:
            args.extend(["--project-dir", self.project_dir])
        if self.models is not None:
            args.extend(["--models", self.models])
        if self.target is not None:
            args.extend(["--target", self.target])

        return self._call("seed", args)

    def generate_docs(self) -> str:
        # The static docs generation requires dbt-core >= 1.7
        args = ["generate", "--static", "--no-compile"]
        if self.project_dir is not None:
            args.extend(["--project-dir", self.project_dir])
        if self.models is not None:
            args.extend(["--models", self.models])
        if self.target is not None:
            args.extend(["--target", self.target])

        return self._call("docs", args)

    def _read_dbt_artifact(self, name: str, raw: bool = False):
        artifact = os.path.join(
            ".",
            self.project_dir or "",
            self._project_config.get("target", "target"),
            name,
        )
        try:
            with open(artifact) as m:
                return m.read() if raw else json.load(m)
        except FileNotFoundError:
            return None

    def _push_state(self):
        # Push new state to DBT_STATE_STORAGE if self.state_store
        if not self.state_store:
            return
        files = {"manifest.json": self.manifest, "run_results.json": self.run_results}
        with S3(s3root=self.state_store) as s3:
            for file, _get in files.items():
                val = _get()
                if val is not None:
                    s3.put(file, json.dumps(val))

    def _pull_state(self, tempdir):
        # Fetch previous state from DBT_STATE_STORAGE if self.state_store
        if not self.state_store:
            return
        with S3(s3root=self.state_store) as s3:
            files = s3.get_all()
            for file in files:
                shutil.copy(file.path, os.path.join(tempdir, file.key))

    def _call(self, cmd, args):
        # Synthesize a profiles.yml from the passed in config dictionary if present.
        with tempfile.TemporaryDirectory() as tempdir:
            profile_args = []
            if self.profiles is not None:
                conf = yaml.dump(self.profiles)
                f = open(os.path.join(tempdir, "profiles.yml"), "w")
                f.write(conf)
                f.close()
                profile_args = ["--profiles-dir", tempdir]

            state_args = []
            if self.state_store:
                state_path = os.path.join(tempdir, "prev_state")
                os.makedirs(state_path)
                state_args = ["--state", state_path]
                self._pull_state(state_path)

            try:
                return subprocess.check_output(
                    [self.bin, cmd] + args + profile_args + state_args,
                    stderr=subprocess.PIPE,
                ).decode()
            except subprocess.CalledProcessError as e:
                raise DBTExecutionFailed(msg=e.output.decode())
            finally:
                # Push artifacts to S3 if self.state_store
                self._push_state()


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
            if not os.path.exists(component_path):
                continue
            for path in glob.glob(os.path.join(component_path, "**"), recursive=True):
                files.append(path)

        return files
