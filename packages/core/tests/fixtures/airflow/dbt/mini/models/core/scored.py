def model(dbt, session):
    dbt.config(materialized="table")
    return dbt.ref("report")
