from metaflow.metaflow_config_funcs import from_conf

###
# CONFIGURE: You can also specify additional debugging options that are
#            activated using the METAFLOW_DEBUG_XXX environment variable. See debug.py
###
DEBUG_OPTIONS = ["dbtdebug"]

# Which adapter to use with DBT. Default to Postgres.
# This will also determine which dependency to install for conda environments as the dbt-[adapter-name] python package.
# see https://docs.getdbt.com/docs/supported-data-platforms for a list of adapters
DBT_ADAPTER_NAME = from_conf("DBT_ADAPTER_NAME", "postgres")


def get_pinned_conda_libs(python_version, datastore_type):
    return {"pyyaml": "6.0", f"dbt-{DBT_ADAPTER_NAME}": "1.7.0"}
