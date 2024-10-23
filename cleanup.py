# Databricks notebook source
# MAGIC %run ./parallel_funcs

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/local_*

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /tmp/asset_generation

# COMMAND ----------

# Note: most list APIs have pagination. You may need to run each function multiple times, especially if there are many objects.
from databricks.sdk import WorkspaceClient
import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from itertools import repeat

w = WorkspaceClient()
numParallel = 32

def contains_uuid(uuid):
    regex = uuid4_pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', re.IGNORECASE)
    match = uuid4_pattern.search(uuid)
    return bool(match)

# COMMAND ----------

def deleteGroup(w, name, id):
    if contains_uuid(name):
        try:
            w.groups.delete(id)
            return id
        except Exception:
            print(f"Could not delete group {id}.")

group_list = list(w.groups.list())
group_ids = [group.id for group in group_list]
group_names = [group.display_name for group in group_list]

with ThreadPoolExecutor(max_workers = numParallel) as executor:
     threads = executor.map(deleteGroup, repeat(w), group_names, group_ids)
     for thread in threads:
          print(f"Deleted group {thread}.")

# COMMAND ----------

def deleteUser(w, name, id):
    if contains_uuid(name):
        try:
            w.users.delete(id)
            return id
        except Exception:
            print(f"Could not delete user {id}.")

user_list = list(w.users.list())
user_ids = [user.id for user in user_list]
user_names = [user.display_name for user in user_list]

with ThreadPoolExecutor(max_workers = numParallel) as executor:
     threads = executor.map(deleteUser, repeat(w), user_names, user_ids)
     for thread in threads:
          print(f"Deleted user {thread}.")

# COMMAND ----------

def deleteCluster(w, name, id):
    if contains_uuid(name):
        try:
            w.clusters.permanent_delete(id)
            return id
        except Exception:
            print(f"Could not delete cluster {id}.")

cluster_list = list(w.clusters.list())
cluster_ids = [cluster.cluster_id for cluster in cluster_list]
cluster_names = [cluster.cluster_name for cluster in cluster_list]

with ThreadPoolExecutor(max_workers = numParallel) as executor:
     threads = executor.map(deleteCluster, repeat(w), cluster_names, cluster_ids)
     for thread in threads:
          print(f"Deleted cluster {thread}.")

# COMMAND ----------

def deleteFolder(w, path):
    if contains_uuid(path):
        try:
            w.workspace.delete(path, recursive=True)
            return path
        except Exception:
            print(f"Could not delete folder {path}.")

folder_list = [folder.path for folder in w.workspace.list("/Users/")]

with ThreadPoolExecutor(max_workers = numParallel) as executor:
     threads = executor.map(deleteFolder, repeat(w), folder_list)
     for thread in threads:
          print(f"Deleted folder {thread}.")

# COMMAND ----------

def deleteJob(w, name, id):
    if contains_uuid(name):
        try:
            w.jobs.delete(id)
            return id
        except Exception:
            print(f"Could not delete job {id}.")
    else:
        print(f"Skipped job {name}")

job_list = list(w.jobs.list())
job_ids = [job.job_id for job in job_list]
job_names = [job.settings.name for job in job_list]

with ThreadPoolExecutor(max_workers = numParallel) as executor:
     threads = executor.map(deleteJob, repeat(w), job_names, job_ids)
     for thread in threads:
          print(f"Deleted job {thread}.")

# COMMAND ----------

def deleteQuery(w, name, id):
    if contains_uuid(name):
        try:
            w.jobs.delete(id)
            return id
        except Exception:
            print(f"Could not delete query {id}.")
    else:
        print(f"Skipped query {name}")

query_list = list(w.queries.list())
query_ids = [query.id for query in query_list]
query_names = [query.display_name for query in query_list]

with ThreadPoolExecutor(max_workers = numParallel) as executor:
     threads = executor.map(deleteQuery, repeat(w), job_names, job_ids)
     for thread in threads:
          print(f"Deleted query {thread}.")

# COMMAND ----------

