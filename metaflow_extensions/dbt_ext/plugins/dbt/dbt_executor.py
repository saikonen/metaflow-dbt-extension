import subprocess

from metaflow.exception import MetaflowException

class DBTExecutionFailed(MetaflowException):
    headline = "DBT Run execution failed"

class DBTExecutor():
    def __init__(self, model: None, project_dir: None):
        self.model = model
        self.project_dir = project_dir
        self.bin = "dbt"

    def execute(self):
        args = ["--fail-fast"]
        if self.project_dir is not None:
            args.extend(["--project-dir",self.project_dir])
        if self.model is not None:
            args.extend(["--model", self.model])

        try:
            return subprocess.check_output([self.bin, "run"] + args, stderr=subprocess.PIPE).decode()
        except subprocess.CalledProcessError as e:
            raise DBTExecutionFailed(msg=e.stderr.decode())
        
