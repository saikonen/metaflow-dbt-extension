from metaflow.metaflow_config_funcs import from_conf
from metaflow.metaflow_config import get_pinned_conda_libs as original_conda_pins

###
# CONFIGURE: You can also specify additional debugging options that are
#            activated using the METAFLOW_DEBUG_XXX environment variable. See debug.py
###
DEBUG_OPTIONS = ["dbtdebug"]

# Which adapter to use with DBT. Default to Postgres.
# This will also determine which dependency to install for conda environments as the dbt-[adapter-name] python package.
# see https://docs.getdbt.com/docs/supported-data-platforms for a list of adapters
DBT_ADAPTER = from_conf("DBT_ADAPTER_NAME", "postgres")


def get_pinned_conda_libs(python_version, datastore_type):
    pins = original_conda_pins(python_version, datastore_type)

    pins.update({"pyyaml": "6.0", f"dbt-{DBT_ADAPTER}": "1.6.2"})
    return pins
