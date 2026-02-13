from dbt.adapters.base import AdapterPlugin

from dbt.adapters.pal.connections import PalCredentials
from dbt.adapters.pal.impl import PalAdapter
from dbt.include import pal

Plugin = AdapterPlugin(
    adapter=PalAdapter, # dbt-core calls __new__() on this class. The resulting instance take charge the actual processing.
    credentials=PalCredentials, # Specifies the class that manages connection info for the data platform
    include_path=pal.PACKAGE_PATH, # dbt/include/pal/__init__.py
)
