from dataclasses import dataclass

from dbt.adapters.contracts.connection import Credentials

@dataclass
class PalCredentials(Credentials):
    # Class that manages connection info for the data platform, specified in the Plugin (src/dbt/adapters/pal/__init__.py)
    # Actual connection info comes from the profile specified by db_profile, so PalCredentials only has minimal fields

    db_profile: str = ""

    # Defined only to satisfy the `Credentials` base class requirements
    database: str = ""
    schema: str = ""

    # Used to identify the adapter type
    @property
    def type(self):
        return "pal"

    # Abstract method of `Credentials``; subclasses must implement this
    @property
    def unique_field(self):
        return self.db_profile

    # Abstract method of `Credentials`; subclasses must implement this
    def _connection_keys(self):
        return ("db_profile",)
