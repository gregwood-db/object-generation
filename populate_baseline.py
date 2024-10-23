# Databricks notebook source
# MAGIC %md
# MAGIC # Baseline Asset Generation Notebook
# MAGIC
# MAGIC This notebook populates an empty workspace with generated assets. It should be run on a completely empty workspace.
# MAGIC
# MAGIC The general workflow is as follows:
# MAGIC - Generate groups (initially empty)
# MAGIC - Generate users (assign each to a random group)
# MAGIC - Generate notebooks (randomly chosen owners/sizes)
# MAGIC - Generate files (randomly chosen owners/sizes)
# MAGIC - Generate clusters (randomly chosen owners, minimal size)
# MAGIC - Generate jobs (randomly chosen owners; new small NB for 2 tasks)
# MAGIC - Generate warehouses
# MAGIC - Generate queries and (manually) dashboards
# MAGIC
# MAGIC The quantity of each object should be set below before running. Each object will be generated in "batches" with a back-off time to replicate steady-state operation, until the total count is reached.
# MAGIC
# MAGIC To run the generation process for each object type, run the `generate_in_chunks` function and supply an object type, object count, batch time, max object count per batch, and thread count. Valid object types are `group`,`user`,`notebook`,`file`,`cluster`,`job`,`warehouse`, and `query`. You can also supply a `base_directory` in which asset logs will be created; by default, the script will create a directory in `dbfs:/tmp/asset_generation`.
# MAGIC
# MAGIC *Note: dashboards should be created manually. It is not currently possible to automatically populate dashboards, even though they can be created via API.*

# COMMAND ----------

# MAGIC %run ./parallel_funcs

# COMMAND ----------

# total count for each object class
num_groups = 1000
num_users = 7500
num_notebooks = 400000
num_files = 25000
num_clusters = 500
num_jobs = 5000
num_warehouses = 5
num_queries = 5000

# batch time in minutes
batch_time = 15

# max objects to generate per batch
max_per_batch = 20000

# parallelization factor
threads = 16

# base directory where logs already exist
base_directory = "dbfs:/tmp/asset_generation"

# COMMAND ----------

# run if using existing users/groups as a baseline

# pull the existing users in the WS and add them to the users log
#pull_existing_users()

# pull the existing groups in the WS and add them to the groups log
#pull_existing_groups()

# COMMAND ----------


# generate groups
generate_in_batch("group", batch_time, max_per_batch, num_groups, threads, base_directory)

# generate users
generate_in_batch("user", batch_time, max_per_batch, num_users, threads, base_directory)

# generate notebooks
generate_in_batch("notebook", batch_time, max_per_batch, num_notebooks, threads, base_directory)

# generate files
generate_in_batch("file", batch_time, max_per_batch, num_files, threads, base_directory)

# generate clusters
generate_in_batch("cluster", batch_time, max_per_batch, num_clusters, threads, base_directory)

# generate jobs
generate_in_batch("job", batch_time, max_per_batch, num_jobs, threads, base_directory)

# generate warehouses
generate_in_batch("warehouse", batch_time, max_per_batch, num_warehouses, threads, base_directory)

# generate queries
generate_in_batch("query", batch_time, max_per_batch, num_queries, threads, base_directory)