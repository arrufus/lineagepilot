"""airflow dag that .airflowignore hides"""
from airflow import DAG
DAG("hidden")
