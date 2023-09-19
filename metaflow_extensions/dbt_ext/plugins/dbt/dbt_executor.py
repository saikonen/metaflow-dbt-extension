import subprocess
from typing import Dict, Optional

from metaflow.exception import MetaflowException
from metaflow.util import which

class DBTExecutionFailed(MetaflowException):
    headline = "DBT Run execution failed"

# TODO: There is a choice to utilize the Python library provided by DBT for performing the run, and accessing run results as well.
# This would introduce a heavy dependency for the decorator use case, which can be completely avoided with the custom implementation
# via calling the CLI via subprocess only at the point when execution needs to happen. Decide on the approach after PoC is complete.
class DBTExecutor():
    def __init__(self, model: str = None, project_dir: str = None):
        self.model = model
        self.project_dir = project_dir
        self.bin = which("./dbt") or which("dbt")
        if self.bin is None:
            raise DBTExecutionFailed("Can not find DBT binary. Please install DBT")

    def run(self) -> str:
        args = ["--fail-fast"]
        if self.project_dir is not None:
            args.extend(["--project-dir",self.project_dir])
        if self.model is not None:
            args.extend(["--model", self.model])
        
        return self._call("run", args)

    def _call(self, cmd, args):
        try:
            return subprocess.check_output([self.bin, cmd] + args, stderr=subprocess.PIPE).decode()
        except subprocess.CalledProcessError as e:
            raise DBTExecutionFailed(msg=e.output.decode())
        
