import inspect
from contextlib import contextmanager
from typing import Set

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.meta import available
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.factory import FACTORY, get_adapter_by_type

from .connections import PalCredentials

@contextmanager
def _release_plugin_lock():
    # FACTORY is a global variable defined by dbt-core, used to register and retrieve Adapters
    # For mutual exclusion, FACTORY methods internally acquire a lock, but calling them when the lock is already held causes a deadlock
    # _release_plugin_lock() temporarily releases the lock to avoid this
    FACTORY.lock.release() # Release the lock
    try:
        yield # Execute the code inside the `with` block
    finally:
        FACTORY.lock.acquire() # Re-acquire the lock after execution


def _find_db_profile(profile_name: str, db_profile_target_name: str):
    # Load a Profile object with the `target` specified by the db_profile field in `profiles.yml`
    # Uses various dbt-core functions to read, parse, and transform `profiles.yml`

    from dbt.config.profile import read_profile, Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.flags import get_flags

    flags = get_flags()
    profile_renderer = ProfileRenderer(getattr(flags, "VARS", {}))

    raw_profiles = read_profile(flags.PROFILES_DIR)
    raw_profile = raw_profiles[profile_name]

    db_profile = Profile.from_raw_profile_info(
        raw_profile=raw_profile,
        profile_name=profile_name,
        renderer=profile_renderer,
        target_override=db_profile_target_name,
    )

    return db_profile


def _find_funcs_in_stack(funcs: Set[str]) -> bool:
    # Returns True if any of the specified function names exist on the call stack
    frame = inspect.currentframe()
    while frame:
        if frame.f_code.co_name in funcs:
            return True
        frame = frame.f_back
    return False


class PalCredentialsWrapper:
    # Wraps a `Credentials` object and only changes the behavior of the `type` property

    def __init__(self, db_credentials):
        # Receives and stores the `Credentials` object for the `target` specified by `db_profile` in `profiles.yml`
        self._db_credentials = db_credentials

    @property
    def type(self):
        # The value `type` should return depends on the context
        # When `type` is referenced during calls to the specified methods, returns `self._db_credentials.type`; otherwise returns `"pal"`
        if _find_funcs_in_stack({"to_target_dict", "db_materialization"}):
            return self._db_credentials.type
        return "pal"

    def __getattr__(self, name):
        # Delegates everything except the type property to `self._db_credentials`
        return getattr(self._db_credentials, name)


class PalAdapterWrapper:
    def __init__(self, db_adapter_type_name: str, config, mp_context=None):
        # Retrieve and hold the Adapter instance (e.g. BigQueryAdapter) that has connection info for the data platform
        self._db_adapter = get_adapter_by_type(db_adapter_type_name)

        # Store the `config` prepared by PalAdapter as-is
        self.config = config

    def type(self):
        # The value `type` should return depends on the context
        # When `type` is referenced during calls to the specified methods, returns `self._db_adapter.type()`; otherwise returns `"pal"`
        if _find_funcs_in_stack({"render", "db_materialization"}):
            return self._db_adapter.type()
        return "pal"

    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        # Called from the materialization macro when processing a Python model
        # parsed_model: model info passed by dbt-core (`database`, `schema`, `alias`, etc.)
        # compiled_code: Python code compiled by the Adapter instance. Various boilerplate is appended to the user's code.

        import re
        print("[pal] Executing Python model via exec()")

        # Strip unnecessary parts from compiled_code
        # pyspark-related lines (from pyspark, import pyspark, spark = ..., spark.conf.set)
        lines = compiled_code.splitlines(keepends=True)
        filtered = [
            line for line in lines
            if not re.match(r"\s*(from pyspark|import pyspark|spark\s*[=.])", line)
        ]
        clean_code = "".join(filtered)

        # `compiled_code` contains `# COMMAND ----------` as a delimiter
        # Split using it as a delimiter, and keep only the first element and the next one
        # The remaining parts are Dataproc-specific code, unnecessary for dbt-pal
        parts = clean_code.split("# COMMAND ----------")
        if len(parts) >= 2:
            clean_code = parts[0] + parts[1]

        # Execute clean_code. This does not invoke the user's Python model code.
        # As a result of execution, functions, classes, variables, etc. defined in `clean_code` are stored in `namespace`.
        # The user's Python model code is stored under the name `model`.
        namespace = {}
        exec(clean_code, namespace)

        # Retrieve and invoke the user's python model code from `namespace`
        # dbtObj requires a function that takes a table name and returns a DataFrame
        # This instance becomes the `dbt` argument (first argument) of the Python model
        model_func = namespace["model"]
        dbt_obj_cls = namespace["dbtObj"]
        dbt_obj = dbt_obj_cls(self._read_df_from_bigquery)
        df = model_func(dbt_obj, None)

        # If the Python model returned something, write the returned pandas DataFrame to BigQuery
        if df is not None:
            self._write_df_to_bigquery(parsed_model, df)

        return AdapterResponse("OK")

    def _write_df_to_bigquery(self, parsed_model: dict, df) -> None:
        # Write the given pandas DataFrame to the specified BigQuery table

        from google.cloud.bigquery import LoadJobConfig, WriteDisposition

        database = parsed_model["database"]
        schema = parsed_model["schema"]
        alias = parsed_model.get("alias", parsed_model["name"])
        table_ref = f"{database}.{schema}.{alias}"

        print(f"[pal] Writing DataFrame to {table_ref}")

        connection = self._db_adapter.connections.get_thread_connection()
        client = connection.handle

        # `WRITE_TRUNCATE`: delete all existing data before writing new data
        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print(f"[pal] Written {job.output_rows} rows to {table_ref}")

    def _read_df_from_bigquery(self, table_name: str):
        # Read data from a BigQuery table and return it as a pandas DataFrame
        # By passing this method to the dbtObj constructor, this method gets executed when `dbt.ref()` is invoked in a Python model

        print(f"[pal] Reading DataFrame from {table_name}")

        connection = self._db_adapter.connections.get_thread_connection()
        client = connection.handle

        sql = f"SELECT * FROM `{table_name}`"
        df = client.query(sql).to_dataframe()

        print(f"[pal] Read {len(df)} rows from {table_name}")
        return df

    @available
    def db_materialization(self, context: dict, materialization: str):
        # Called from the materialization macro when processing a SQL model

        from dbt.clients.jinja import MacroGenerator
        from dbt.parser.manifest import ManifestLoader

        # The table materialization macro provided by the Adapter that `self._db_adapter.type()` returns is stored in `materialization_macro`
        manifest = ManifestLoader.get_full_manifest(self.config)
        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name, materialization, self._db_adapter.type()
        )

        # Generate an executable function from `materialization_macro`, then execute it
        # Its return value directly becomes the return value of db_materialization()
        return MacroGenerator(
            materialization_macro, context, stack=context["context_macro_stack"]
        )()

    def __getattr__(self, name):
        # Delegate all undefined property and method calls to `self._db_adapter`
        return getattr(self._db_adapter, name)


class PalAdapter(BaseAdapter):
    # This class is specified in the Plugin (src/dbt/adapters/pal/__init__.py), so dbt-core calls __new__() defined here

    def __new__(cls, config, mp_context=None):
        # Get the `db_profile` value from `profiles.yml` and store it in db_profile_target_name
        pal_credentials: PalCredentials = config.credentials
        db_profile_target_name = pal_credentials.db_profile

        # Raise an error if the `db_profile` property in `profiles.yml` is not set
        assert db_profile_target_name, (
            "pal credentials must have a `db_profile` property set"
        )

        with _release_plugin_lock():
            # Get the `Credentials` for the `target` specified by the db_profile field in `profiles.yml`
            # For `type: bigquery`, this will be a `BigQueryCredentials` instance
            db_profile = _find_db_profile(
                config.profile_name,
                db_profile_target_name,
            )
            db_credentials = db_profile.credentials

            if db_credentials.type != "bigquery":
                raise NotImplementedError(
                    f"dbt-pal currently only supports BigQuery, but db_profile"
                    f" '{db_profile_target_name}' has type '{db_credentials.type}'"
                )

            # Add the Adapter pointed to by db_credentials.type to the macro search path
            # This makes materialization macros searched in the order: pal -> bigquery -> default
            pal_plugin = FACTORY.get_plugin_by_name("pal")
            pal_plugin.dependencies = [db_credentials.type]

            # Register the `Plugin` defined by the Adapter (e.g. dbt-bigquery) that db_credentials.type refers to
            FACTORY.load_plugin(db_credentials.type)

            # We want to register the `Adapter` class that the Adapter (e.g. dbt-bigquery) pointed to by db_credentials.type defines
            # However, FACTORY.register_adapter() takes config as its argument
            # So we temporarily replace `config`.`credentials` with `db_credentials` for registration
            config.credentials = db_credentials
            FACTORY.register_adapter(config, mp_context)

            # dbt-core looks up Adapters based on config.credentials.type, so we cannot leave db_credentials as-is
            # But the original `PalCredentials` in `config.credentials` is also problematic
            # since it is a `Credentials` with almost no info besides `db_profile` and lacks the actual data platform connection info
            # Therefore we set PalCredentialsWrapper instead
            config.credentials = PalCredentialsWrapper(db_credentials)

            # Returning a `PalAdapter` instance would trigger `BaseAdapter`'s `__init__()` method
            # Normally that is fine, but `PalAdapter` has no connection info and would not work properly
            # So we return a `PalAdapterWrapper` instance instead
            # `PalAdapter` is what dbt-core's plugin system recognizes, but `PalAdapterWrapper` is what actually runs
            return PalAdapterWrapper(db_credentials.type, config, mp_context)

    # Implemented because dbt-core's internal processing requires it
    @classmethod
    def type(cls):
        return "pal"
