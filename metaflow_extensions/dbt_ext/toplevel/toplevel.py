__mf_extensions__ = "dbt-ext"

# Make the switch decorator available at the top level.
from ..plugins.dbt import dbt_deco as dbt

import pkg_resources

try:
    __version__ = pkg_resources.get_distribution("metaflow-dbt-extension").version
except:
    # this happens on remote environments since the job package
    # does not have a version
    __version__ = None
