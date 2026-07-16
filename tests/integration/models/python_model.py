def model(dbt, session):
    source_df = dbt.source("src", "test_table")
    ref_df = dbt.ref("sql_model_view")
    multiplier = dbt.config.get("multiplier")

    ref_df["value"] = (source_df["value"] + ref_df["value"]) * multiplier
    return ref_df
