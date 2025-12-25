# Databricks notebook source
# Orchestration Notebook

print("Starting Orchestration...")

# Run Notebook 1
print("Running Notebook 1: data_ingestion")
result1 = dbutils.notebook.run("data_ingestion", timeout_seconds=300)
print("Notebook 1 Output:", result1)

# Run Notebook 2
print("Running Notebook 2: delta_processing")
result2 = dbutils.notebook.run("delta_processing", timeout_seconds=300)
print("Notebook 2 Output:", result2)

print("✔ Workflow Completed Successfully!")
