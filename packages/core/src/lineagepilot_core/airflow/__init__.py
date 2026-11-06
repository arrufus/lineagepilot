"""Airflow linkage: read DAG files with the AST only, and link their tasks to dbt nodes.

DAG files are untrusted workspace input. They are never imported or executed (design section 6.2).
Grown from spike S3, whose README has the evidence and the Cosmos task-id mapping.
"""
