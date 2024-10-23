# Databricks notebook source
# MAGIC %fs
# MAGIC ls /tmp/asset_generation/file_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/tmp/asset_generation/notebook_logs`

# COMMAND ----------

