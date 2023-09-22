import subprocess
import os
import json
from typing import Dict, Optional

from metaflow.exception import MetaflowException
from metaflow.util import which

class DBTExecutionFailed(MetaflowException):
    headline = "DBT Run execution failed"

# TODO: There is a choice to utilize the Python library provided by DBT for performing the run, and accessing run results as well.
# This would introduce a heavy dependency for the decorator use case, which can be completely avoided with the custom implementation
# via calling the CLI via subprocess only at the point when execution needs to happen. Decide on the approach after PoC is complete.
class DBTExecutor():
    def __init__(self, model: str = None, project_dir: str = None, target: str = None):
        self.model = model
        self.project_dir = project_dir
        self.target = target
        self.bin = which("./dbt") or which("dbt")
        if self.bin is None:
            raise DBTExecutionFailed("Can not find DBT binary. Please install DBT")

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
            args.extend(["--project-dir",self.project_dir])
        if self.model is not None:
            args.extend(["--model", self.model])
        if self.target is not None:
            args.extend(["--target", self.target])
        
        return self._call("run", args)

    def _read_dbt_artifact(self, name: str):
        artifact = os.path.join(".", self.project_dir or "", "target", name)
        try:
            with open(artifact) as m:
                return json.load(m)
        except FileNotFoundError:
            return None

    def _call(self, cmd, args):
        try:
            return subprocess.check_output([self.bin, cmd] + args, stderr=subprocess.PIPE).decode()
        except subprocess.CalledProcessError as e:
            raise DBTExecutionFailed(msg=e.output.decode())
        
