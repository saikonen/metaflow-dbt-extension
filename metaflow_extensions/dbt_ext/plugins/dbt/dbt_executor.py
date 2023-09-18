import subprocess

from metaflow.exception import MetaflowException
from metaflow.util import which

class DBTExecutionFailed(MetaflowException):
    headline = "DBT Run execution failed"

class DBTExecutor():
    def __init__(self, model: None, project_dir: None):
        self.model = model
        self.project_dir = project_dir
        self.bin = which("./dbt") or which("dbt")
        if self.bin is None:
            raise DBTExecutionFailed("Can not find DBT binary. Please install DBT")

    def run(self):
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
            raise DBTExecutionFailed(msg=e.stderr.decode())
        
