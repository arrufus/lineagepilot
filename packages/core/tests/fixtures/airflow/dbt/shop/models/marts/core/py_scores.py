def model(dbt, session):
    dbt.config(materialized="table", tags=["ml"])
    return dbt.ref("dim_customers")
