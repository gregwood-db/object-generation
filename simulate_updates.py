# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Object Update Simulation Notebook
# MAGIC
# MAGIC This notebook simulates updates to various workspace objects at a tunable frequency and volume.
# MAGIC
# MAGIC The following object types will be updated as the notebook runs:
# MAGIC - Notebooks
# MAGIC - Files
# MAGIC - Clusters
# MAGIC - Jobs
# MAGIC
# MAGIC Notebooks and files will be updated with new content, but the owners and paths will remain the same. Clusters and jobs will be assigned new, randomly selected owners, and clusters will also receive a new cluster policy.
# MAGIC
# MAGIC To run this notebook, provide the `per_batch` number for each object type, the `batch_time` (in minutes), the total `num_batches` (i.e., the script will run for `batch_time*num_batches`), the number of `threads` to use for parallel API calls, and the `base_directory`, which should be the same directory that `populate_baseline` wrote logs when generating the initial objects.
# MAGIC
# MAGIC If you would like to add generation of objects to each batch, instead of just updates, alter the `update_in_batch` function to add calls to functions such as `updateNotebookPar`. See generate_in_batch for examples.

# COMMAND ----------

# MAGIC %run ./parallel_funcs

# COMMAND ----------

# total time for each batch
batch_time = 15

# total count for each object class in each batch
nb_per_batch = 10000
files_per_batch = 2500
clusters_per_batch = 50
jobs_per_batch = 500

# parallelization factor
threads = 32

# base directory where logs already exist
base_directory = "dbfs:/tmp/asset_generation"

# total number of batches
num_batches = 24

# COMMAND ----------

update_in_batch(batch_time, clusters_per_batch, jobs_per_batch, files_per_batch, nb_per_batch, threads, base_directory, num_batches)

# COMMAND ----------

