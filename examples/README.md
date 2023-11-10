# DBT Decorator usage examples

## Setup

The examples require valid DBT projects to be present in order for the flows to run. You can bootstrap the required DBT projects by running
```sh
./setup.sh
```
which will create the following projects
- `examples/dbt_project`
- `examples/jaffle_shop`

along with a placeholder `config.py` which you can fill with further constants as needed.


## Local DBT run

The DBT projects are configured to run against a Postgres instance. If you don't have one readily available, you can spin one up with Docker by running the following command in the `examples/` directory
```sh
docker-compose up
```

Once a Postgres database is up and running, the following flow should complete successfully:
```sh
python dbtflow.py --environment conda --metadata local --datastore local run
```

## Remote execution

For our example we use AWS Secrets Manager for storing credentials to an RDS Postgres instance. You need to replace the values in `config.py` with the correct secret key, and db host. When this is done, the following flow should execute successfully.

```sh
python remotedbtflow.py --environment conda run --with kubernetes
```

the flow should work just as well with `batch`, `step-functions` and `argo-workflow`

Alternatively you can define a base Docker image that has the necessary requirements for executing DBT. You can try using the official DBT images as follows:

```sh
export METAFLOW_DEFAULT_CONTAINER_REGISTRY="ghcr.io/dbt-labs"
export METAFLOW_DEFAULT_CONTAINER_IMAGE="dbt-postgres:1.7.0"
python remotedbtflow.py run --with kubernetes
```

Note: DBT version 1.7.0 is the minimum required version to support generating documentation as part of the flow with `generate_docs=True`


## Utility: Create a missing database with a flow

If you happen to have a database instance that is not easily accessible from the outside, and you are missing the necessary `dbt_decorator` database from the instance, there is a convenient utility flow provided: `createdbflow.py`

After editing the `config.py`, you should be able to run the flow, which will try to create the missing database on the instance if the IAM role has sufficient permissions.

```sh
python createdbflow.py --environment conda run --with kubernetes
```