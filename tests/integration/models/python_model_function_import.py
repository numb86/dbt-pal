def model(dbt, session):
    # A function-body import runs when model() is called, not when the compiled
    # code is exec()'d, so the project root must still be on sys.path at call time.
    from lazy_utils import add_twenty

    df = dbt.source("src", "test_table")
    df["value"] = add_twenty(df["value"])
    return df
