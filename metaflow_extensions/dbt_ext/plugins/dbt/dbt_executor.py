import subprocess

from metaflow.exception import MetaflowException

class DBTExecutionFailed(MetaflowException):
    headline = "DBT Run execution failed"

class DBTExecutor():
    def __init__(self, project_dir: None):
        self.project_dir = project_dir
        self.bin = "dbt"

    def execute(self):
        args = []
        if self.project_dir is not None:
            args.extend([f"--project-dir",self.project_dir])

        try:
            return subprocess.check_output([self.bin, "run"] + args, stderr=subprocess.PIPE).decode()
        except subprocess.CalledProcessError as e:
            raise DBTExecutionFailed(msg=e.stderr.decode())
        
