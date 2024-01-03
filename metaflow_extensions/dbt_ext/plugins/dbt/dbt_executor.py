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
        state_prefix: str = None,
        ds_type=None,
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
        self.datastore = None
        self.state_prefix = state_prefix
        if self.state_prefix:
            self._init_datastore(ds_type)

    def _init_datastore(self, ds_type):
        from metaflow.plugins import DATASTORES

        datastore = [d for d in DATASTORES if d.TYPE == ds_type][0]

        root = datastore.get_datastore_root_from_config(print)
        self.datastore = datastore(f"{root}/dbt_state/{self.state_prefix}")

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
        # Push new state to self.datastore if configured
        if not self.datastore:
            return
        files_and_paths = {
            key: os.path.join(
                ".",
                self.project_dir or "",
                self._project_config.get("target", "target"),
                key,
            )
            for key in ["manifest.json", "run_results.json"]
        }
        files_and_handles = {
            key: open(path, mode="rb")
            for key, path in files_and_paths.items()
            if os.path.exists(path)
        }

        self.datastore.save_bytes(
            (name, content) for name, content in files_and_handles.items()
        )

    def _pull_state(self, tempdir):
        # Fetch previous state to tempdir from self.datastore if configured
        if not self.datastore:
            return
        with self.datastore.load_bytes(["manifest.json", "run_results.json"]) as result:
            for key, file, _ in result:
                if file is not None:
                    shutil.move(file, os.path.join(tempdir, key))

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
            # If datastore is configured, we intend to use a previous state.
            if self.datastore:
                state_path = os.path.join(tempdir, "prev_state")
                os.makedirs(state_path)
                self._pull_state(state_path)
                if os.listdir(state_path):
                    # a previous state was available.
                    state_args = ["--state", state_path]
                else:
                    # we do not have a previous state,
                    # so we need to clean up any known state selectors from args in order to avoid errors.
                    def _cleanup(arg: str):
                        # TODO: Add debug message.
                        split = arg.split(",")
                        args = [
                            a for a in split if not "state:" in a and not "result:" in a
                        ]
                        return ",".join(args)

                    args = [_cleanup(arg) for arg in args]

            try:
                return subprocess.check_output(
                    [self.bin, cmd] + args + profile_args + state_args,
                    stderr=subprocess.PIPE,
                ).decode()
            except subprocess.CalledProcessError as e:
                raise DBTExecutionFailed(msg=e.output.decode())
            finally:
                # Push state artifacts to self.datastore
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
