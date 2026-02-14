from utils.helpers import multiply


def model(dbt, session):
    df = dbt.source("src", "test_table")
    df["value"] = multiply(df["value"], 2)
    return df
