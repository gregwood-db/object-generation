# Databricks notebook source
# MAGIC %run ./helper_funcs

# COMMAND ----------

import os
import uuid
import time
import random
import functools
import pandas as pd
from pathlib import Path
from math import floor, inf
from itertools import repeat
from numpy.random import normal
from pyspark.sql.types import StringType
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

# set to avoid driver memory heap failures
max_obj_to_load = 250000

# COMMAND ----------

def log(_func=None, *, log_path = None):

    '''
    Logging decorator to provide tracking of created assets.

    log_path is provided to the decorator within each parallelized function.
    '''

    # helper to write to log location
    def write_local_log(path, value):
        with open (path, "a") as file:
            file.write(f"{value}\n")

    def decorator_log(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            if log_path is None:
                raise Exception("log_path must be provided.")

            try:
                result = func(*args, **kwargs)
                if result:
                    write_local_log(log_path, result)
                return result
            except Exception as e:
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)

# COMMAND ----------

def createGroupPar(w, group_log_path, num_obj, num_exec, debug=False, local_path="/tmp/local_group_log"):
    
    '''
    A parallelized version of createGroup that also includes logging.

    Arguments:
    w -- a WorkspaceClient object
    num_obj -- the number of groups to create
    group_log_path -- the path where a log of all created groups will be written
    num_exec -- the number of parallel threads to spawn
    '''

    # create the local log path where intermediate logs will be written
    log_uuid = str(uuid.uuid4())
    Path(local_path).mkdir(parents=True, exist_ok=True)
    local_log = os.path.join(local_path, f"log_{log_uuid}.txt")

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(group_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(group_log_path)

    # create a decorated version of createUser that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    @log(log_path=local_log)
    def createGroupLog(_, w):
        return createGroup(w)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createGroupLog,
                               dummy_vec,
                               repeat(w))
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created group {thread}.")
            else:
                pass

    # write the complete log to dbfs
    pd_df = pd.read_csv(local_log, header=None, names=["value"], dtype=str)
    log_df = spark.createDataFrame(pd_df)
    log_df.write.mode("append").format("delta").save(group_log_path)

# COMMAND ----------

def createUserPar(w, group_log_path, user_log_path, num_obj, num_exec, debug=False, local_path="/tmp/local_user_log"):
    
    '''
    A parallelized version of createUser that also includes logging.
    Each user will be added to a random group.

    Arguments:
    w -- a WorkspaceClient object
    num_obj -- the number of users to create
    user_log_path -- the path where a log of all created users will be written
    num_exec -- the number of parallel threads to spawn
    '''

    # create the local log path where intermediate logs will be written
    log_uuid = str(uuid.uuid4())
    Path(local_path).mkdir(parents=True, exist_ok=True)
    local_log = os.path.join(local_path, f"log_{log_uuid}.txt")

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(user_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(user_log_path)

    # create a decorated version of createUser that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    @log(log_path=local_log)
    def createUserLog(_, w, group):
        return createUser(w, group)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a list of owners, randomly sampled, from the user log
    group_list = (spark.read.format("delta")
                      .load(group_log_path)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createUserLog,
                               dummy_vec,
                               repeat(w),
                               group_list)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created user {thread}.")
            else:
                pass

    # write the complete log to dbfs
    pd_df = pd.read_csv(local_log, header=None, names=["value"], dtype=str)
    log_df = spark.createDataFrame(pd_df)
    log_df.write.mode("append").format("delta").save(user_log_path)
                    

# COMMAND ----------

def createNotebookPar(w, user_log_path, nb_log_path, num_obj, num_exec, mean_nb_size=80000, debug=False, local_path="/tmp/local_nb_log"):
    
    '''
    Parallelized version of createOrUpdateNotebook that also includes logging.
    A random owner will be assigned to each notebook of size sampled a normal centered at mean_nb_size.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of notebooks to create
    nb_log_path -- the path where a log of all created notebook IDs will be written
    num_exec -- the number of parallel threads to spawn
    mean_nb_size -- the mean size, in bytes, of all notebooks
    '''

    # create the local log path where intermediate logs will be written
    log_uuid = str(uuid.uuid4())
    Path(local_path).mkdir(parents=True, exist_ok=True)
    local_log = os.path.join(local_path, f"log_{log_uuid}.txt")

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(nb_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(nb_log_path)

    # create a decorated version of createOrUpdateNotebook that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    @log(log_path=local_log)
    def createNotebookLog(_, w, size, owner):
        return createOrUpdateNotebook(w, size, owner=owner)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a list of owners, randomly sampled, from the user log
    nb_owners = (spark.read.format("delta")
                      .load(user_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())
    
    # generate random NB sizes; for any that are <1024 (1KB), round up
    nb_size = [max(1024, x) for x in [round(x) for x in normal(mean_nb_size, mean_nb_size/5, num_obj)]]

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createNotebookLog,
                               dummy_vec,
                               repeat(w),
                               nb_size,
                               nb_owners)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created notebook {thread}.")
            else:
                pass

    # write the complete log to dbfs
    pd_df = pd.read_csv(local_log, header=None, names=["value"], dtype=str)
    log_df = spark.createDataFrame(pd_df)
    log_df.write.mode("append").format("delta").save(nb_log_path)

# COMMAND ----------

def updateNotebookPar(w, user_log_path, nb_log_path, num_obj, num_exec, mean_nb_size=80000, debug=False):
    
    '''
    Parallelized version of createOrUpdateNotebook that updates existing notebooks.
    A random size sampled from a normal centered mean_nb_size will be used.

    Note that we do not create or update logs for this function, since the file
    key is the path, which already exists.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of notebooks to update
    nb_log_path -- the path where a log of all created notebook IDs are written
    num_exec -- the number of parallel threads to spawn
    mean_nb_size -- the mean size, in bytes, of all notebooks
    '''

    # create a local version of createOrUpdateNotebook that accepts an additional input
    # this input is thrown away, but is used for the total # of objects to be created
    def updateNotebookLocal(_, w, size, path):
        return createOrUpdateNotebook(w, size, path=path)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a random list of notebook paths from already-existing nbs
    nb_paths = (spark.read.format("delta")
                      .load(nb_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())
    
    # generate random NB sizes; for any that are <1024 (1KB), round up
    nb_size = [max(1024, x) for x in [round(x) for x in normal(mean_nb_size, mean_nb_size/5, num_obj)]]

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(updateNotebookLocal,
                               dummy_vec,
                               repeat(w),
                               nb_size,
                               nb_paths)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Updated notebook {thread}.")
            else:
                pass

# COMMAND ----------

def createFilePar(w, user_log_path, file_log_path, num_obj, num_exec, mean_file_size=100000, debug=False, local_path="/tmp/local_file_log"):
    
    '''
    Parallelized version of createOrUpdateFile that also includes logging.
    A random owner will be assigned to each file of size sampled a normal centered at mean_file_size.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of files to create
    file_log_path -- the path where a log of all created file IDs will be written
    num_exec -- the number of parallel threads to spawn
    mean_file_size -- the mean size, in bytes, of all files
    '''

    # create the local log path where intermediate logs will be written
    log_uuid = str(uuid.uuid4())
    Path(local_path).mkdir(parents=True, exist_ok=True)
    local_log = os.path.join(local_path, f"log_{log_uuid}.txt")

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(file_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(file_log_path)

    # create a decorated version of createOrUpdateFile that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    @log(log_path=local_log)
    def createFileLog(_, w, size, owner):
        return createOrUpdateFile(w, size, owner=owner)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a list of owners, randomly sampled, from the user log
    file_owners = (spark.read.format("delta")
                      .load(user_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())
    
    # generate random file sizes; for any that are <1024 (1KB), round up
    file_size = [max(1024, x) for x in [round(x) for x in normal(mean_file_size, mean_file_size/5, num_obj)]]

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createFileLog,
                               dummy_vec,
                               repeat(w),
                               file_size,
                               file_owners)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created file {thread}.")
            else:
                pass

    # write the complete log to dbfs
    pd_df = pd.read_csv(local_log, header=None, names=["value"], dtype=str)
    log_df = spark.createDataFrame(pd_df)
    log_df.write.mode("append").format("delta").save(file_log_path)

# COMMAND ----------

def updateFilePar(w, user_log_path, file_log_path, num_obj, num_exec, mean_file_size=100000, debug=False):
    
    '''
    Parallelized version of createOrUpdateFile that updates file content and owner.
    A random size sampled from a normal centered mean_file_size will be used.

    Note that we do not create or update logs for this function, since the file
    key is the path, which already exists.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of files to update
    file_log_path -- the path where a log of all created file IDs is written
    num_exec -- the number of parallel threads to spawn
    mean_file_size -- the mean size, in bytes, of all files
    '''

    # create a local version of createOrUpdateFile that accepts an additional input
    # this input is thrown away, but is used for the total # of objects to be created
    def updateFileLocal(_, w, size, path):
        return createOrUpdateFile(w, size, path=path)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))
    
    # get a random list of file paths from already-existing files
    file_paths = (spark.read.format("delta")
                      .load(file_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())
    
    # generate random file sizes; for any that are <1024 (1KB), round up
    file_size = [max(1024, x) for x in [round(x) for x in normal(mean_file_size, mean_file_size/5, num_obj)]]

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(updateFileLocal,
                               dummy_vec,
                               repeat(w),
                               file_size,
                               file_paths)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Updated file {thread}.")
            else:
                pass

# COMMAND ----------

def createClusterPar(w, user_log_path, cluster_log_path, num_obj, num_exec, debug=False, local_path="/tmp/local_cluster_log"):
    
    '''
    Parallelized version of createOrUpdateCluster that also includes logging.
    A random owner will be assigned to each cluster.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of clusters to create
    cluster_log_path -- the path where a log of all created cluster IDs will be written
    num_exec -- the number of parallel threads to spawn
    '''

    # create the local log path where intermediate logs will be written
    log_uuid = str(uuid.uuid4())
    Path(local_path).mkdir(parents=True, exist_ok=True)
    local_log = os.path.join(local_path, f"log_{log_uuid}.txt")

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(cluster_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(cluster_log_path)

    # create a decorated version of createOrUpdateCluster that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    @log(log_path=local_log)
    def createClusterLog(_, w, policy, owner):
        return createOrUpdateCluster(w, policy, owner)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # create a cluster policy to apply to these clusters
    cluster_policy = createClusterPolicy(w)

    # get a list of owners, randomly sampled, from the user log
    cluster_owners = (spark.read.format("delta")
                      .load(user_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createClusterLog,
                               dummy_vec,
                               repeat(w),
                               repeat(cluster_policy.policy_id),
                               cluster_owners)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created cluster {thread}.")
            else:
                pass

    # write the complete log to dbfs
    pd_df = pd.read_csv(local_log, header=None, names=["value"], dtype=str)
    log_df = spark.createDataFrame(pd_df)
    log_df.write.mode("append").format("delta").save(cluster_log_path)

# COMMAND ----------

def updateClusterPar(w, user_log_path, cluster_log_path, num_obj, num_exec, debug=False):
    
    '''
    Parallelized version of createOrUpdateCluster that updates existing clusters. The
    owner and cluster policy will be updated.

    Note that we do not create or update logs for this function, since the cluster
    key is the ID, which already exists.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of clusters to create
    cluster_log_path -- the path where a log of all created cluster IDs will be written
    num_exec -- the number of parallel threads to spawn
    '''

    # create a decorated version of createOrUpdateCluster that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    def updateClusterLocal(_, w, policy, owner, cluster_id):
        return createOrUpdateCluster(w, policy, owner=owner, cluster_id=cluster_id)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # create a cluster policy to apply to these clusters
    cluster_policy = createClusterPolicy(w)

    # get a list of owners, randomly sampled, from the user log
    cluster_owners = (spark.read.format("delta")
                      .load(user_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())
    
    # get a list of cluster IDs, randomly sampled, from the cluster log
    cluster_ids = (spark.read.format("delta")
                      .load(cluster_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(updateClusterLocal,
                               dummy_vec,
                               repeat(w),
                               repeat(cluster_policy.policy_id),
                               cluster_owners,
                               cluster_ids)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Updated cluster {thread}.")
            else:
                pass

# COMMAND ----------

def createJobPar(w, user_log_path, job_log_path, num_obj, num_exec, debug=False, local_path="/tmp/local_job_log"):
    
    '''
    Parallelized version of createJob that also includes logging.
    A random owner will be assigned to each job.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of jobs to create
    job_log_path -- the path where a log of all created job IDs will be written
    num_exec -- the number of parallel threads to spawn
    '''

    # create the local log path where intermediate logs will be written
    log_uuid = str(uuid.uuid4())
    Path(local_path).mkdir(parents=True, exist_ok=True)
    local_log = os.path.join(local_path, f"log_{log_uuid}.txt")

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(job_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(job_log_path)

    # create a decorated version of createJob that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    @log(log_path=local_log)
    def createJobLog(_, w, owner):
        return createJob(w, owner)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a list of owners, randomly sampled, from the user log
    job_owners = (spark.read.format("delta")
                      .load(user_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createJobLog,
                               dummy_vec,
                               repeat(w),
                               job_owners)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created job {thread}.")
            else:
                pass

    # write the complete log to dbfs
    pd_df = pd.read_csv(local_log, header=None, names=["value"], dtype=str)
    log_df = spark.createDataFrame(pd_df)
    log_df.write.mode("append").format("delta").save(job_log_path)

# COMMAND ----------

def updateJobPar(w, user_log_path, job_log_path, num_obj, num_exec, debug=False):
    
    '''
    Parallelized version of updateJob.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of jobs to create
    job_log_path -- the path where a log of all created job IDs will be written
    num_exec -- the number of parallel threads to spawn
    '''

    # create a local version of createJob that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    def updateJobLocal(_, w, job_id, owner):
        return updateJob(w, job_id, owner)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a list of owners, randomly sampled, from the user log
    job_owners = (spark.read.format("delta")
                      .load(user_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())
    
    # get a list of owners, randomly sampled, from the user log
    job_ids = (spark.read.format("delta")
                      .load(job_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(updateJobLocal,
                               dummy_vec,
                               repeat(w),
                               job_ids,
                               job_owners)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Updated job {thread}.")
            else:
                pass

# COMMAND ----------

def createWarehousePar(w, user_log_path, wh_log_path, num_obj, num_exec, debug=False, local_path="/tmp/local_wh_log"):
    
    '''
    Parallelized version of createWarehouse that also includes logging.
    A random owner will be assigned to each warehouse.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path to the log of users, as generated by createUserPar
    num_obj -- the number of warehouses to create
    wh_log_path -- the path where a log of all created warehouse IDs will be written
    num_exec -- the number of parallel threads to spawn
    '''

    # create the local log path where intermediate logs will be written
    log_uuid = str(uuid.uuid4())
    Path(local_path).mkdir(parents=True, exist_ok=True)
    local_log = os.path.join(local_path, f"log_{log_uuid}.txt")

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(wh_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(wh_log_path)

    # create a decorated version of createWarehouse that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    @log(log_path=local_log)
    def createWarehouseLog(_, w, owner):
        return createWarehouse(w, owner)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a list of owners, randomly sampled, from the user log
    wh_owners = (spark.read.format("delta")
                      .load(user_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createWarehouseLog,
                               dummy_vec,
                               repeat(w),
                               wh_owners)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created warehouse {thread}.")
            else:
                pass

    # write the complete log to dbfs
    pd_df = pd.read_csv(local_log, header=None, names=["value"], dtype=str)
    log_df = spark.createDataFrame(pd_df)
    log_df.write.mode("append").format("delta").save(wh_log_path)

# COMMAND ----------

def createQueryPar(w, wh_log_path, num_obj, num_exec, debug=False):
    
    '''
    Parallelized version of createQuery that also includes logging.
    Each query will be assigned a random warehouse.

    Arguments:
    w -- a WorkspaceClient object
    wh_log_path -- the path to the log of warehouses, as generated by createWarehousePar
    num_obj -- the number of queries to create
    num_exec -- the number of parallel threads to spawn
    '''

    # create a local version of createQuery that also accepts a second input
    # this input is thrown away, but is used for the total # of objects to be created
    def createQueryLog(_, w, whid):
        return createQuery(w, whid)
    
    # create a list to pass to ThreadPoolExecutor with the total # of objects
    dummy_vec = list(range(num_obj))

    # get a list of owners, randomly sampled, from the user log
    wh_list = (spark.read.format("delta")
                      .load(wh_log_path)
                      .limit(max_obj_to_load)
                      .toPandas()
                      .sample(num_obj, replace=True)['value'].to_list())

    # spawn the threads to run createUser
    with ThreadPoolExecutor(max_workers = num_exec) as executor:
        threads = executor.map(createQueryLog,
                               dummy_vec,
                               repeat(w),
                               wh_list)
        
        # wait for threads to complete before returning
        for thread in threads:
            if debug:
                print(f"Created query {thread}.")
            else:
                pass

# COMMAND ----------

def generate_in_batch(asset_type, batch_time, 
                      max_per_batch, obj_count, threads, 
                      base_directory="dbfs:/tmp/asset_generation"):

    '''
    Function to batch up the creation of objects to simulate realistic usage, 
    and reduce pressure on the workspace limits.

    Arguments:
    asset_type -- a valid asset type: group, user, notebook, file, cluster, job, warehouse, query, dashboard
    batch_time -- the time to wait between batches
    max_per_batch -- the maxium number of objects created within a single batch
    obj_count -- the total number of objects to be created across all batches
    threads -- the number of threads to be used when creating objects
    base_directory -- [optional] the base folder in which logging folders will be created
    '''

    # helper function to chunk up the object creation according to params
    def loop_until_done(func, w, asset_type, batch_time, max_per_batch, obj_count, *args, **kwargs):

        # determine total number of batches, and objects per batch
        # last batch will have the remainder (if any)
        if obj_count <= max_per_batch:
            num_batches = 1
        elif obj_count % max_per_batch == 0:
            num_batches = floor(obj_count / max_per_batch)
            obj_per_batch = [obj_count // num_batches]*num_batches
        else:
            num_batches = floor(obj_count / max_per_batch)
            obj_per_batch = [obj_count // num_batches]*num_batches
            obj_per_batch.append(obj_count % max_per_batch)
            num_batches = num_batches+1

        print(f"Generating assets of type {asset_type}...")
        print(f"A total of {num_batches} batches will be run. Backoff time is set to {batch_time} minutes.")
        
        if num_batches==1:
            # for a single batch, just run all objects at once
            print("Running batch 1...")
            func(w=w, num_obj=obj_count, **kwargs)
        else:
            # for multiple batches, iterate through the batch count list
            batch_count = 0
            curr_obj_count = obj_per_batch[batch_count]

            while batch_count < num_batches:
                print(f"Running batch {batch_count+1} of {num_batches}...")

                # time the creation process and call the passed function
                start_time = time.perf_counter()
                func(w=w, num_obj=curr_obj_count, **kwargs)
                end_time = time.perf_counter()

                if end_time - start_time > batch_time*60:
                    # if we have exceeded the batch time, we likely need to increase the perf characteristics
                    print("WARNING: process time was greater than batch time. \nConsider increasing threads, max_per_batch, or batch_time.")
                    batch_count += 1
                else:
                    # otherwise, sleep for the remainder of the batch time
                    if batch_count < num_batches-1:
                        time_to_wait = batch_time*60 - (end_time - start_time)
                        print(f"Processed batch {batch_count+1}. Waiting {round(time_to_wait)} seconds for next batch...")
                        time.sleep(time_to_wait)
                    batch_count += 1
            
            print(f"Completed generating {asset_type} objects.")


    # create WorkspaceClient to execute the API calls
    w = WorkspaceClient()

    # set to True in order to get lower-level asset creation logs
    debug = False

    # skip this object if obj_count is <=0
    if obj_count <= 0:
        return

    # parse asset type and call appropriate function
    if asset_type == "group":
        group_log_path=os.path.join(base_directory, "group_logs/")
        loop_until_done(createGroupPar, 
                        w,
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        group_log_path=group_log_path,
                        debug=debug)
    elif asset_type == "user":
        group_log_path=os.path.join(base_directory, "group_logs/")
        user_log_path=os.path.join(base_directory, "user_logs/")
        loop_until_done(createUserPar, 
                        w,
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        group_log_path=group_log_path,
                        user_log_path=user_log_path,
                        debug=debug)
    elif asset_type == "notebook":
        nb_log_path=os.path.join(base_directory, "notebook_logs/")
        user_log_path=os.path.join(base_directory, "user_logs/")
        loop_until_done(createNotebookPar,
                        w, 
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        user_log_path=user_log_path,
                        nb_log_path=nb_log_path,
                        debug=debug)
    elif asset_type == "file":
        file_log_path=os.path.join(base_directory, "file_logs/")
        user_log_path=os.path.join(base_directory, "user_logs/")
        loop_until_done(createFilePar,
                        w, 
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        user_log_path=user_log_path,
                        file_log_path=file_log_path,
                        debug=debug)
    elif asset_type == "cluster":
        cluster_log_path=os.path.join(base_directory, "cluster_logs/")
        user_log_path=os.path.join(base_directory, "user_logs/")
        loop_until_done(createClusterPar, 
                        w,
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        user_log_path=user_log_path,
                        cluster_log_path=cluster_log_path,
                        debug=debug)
    elif asset_type == "job":
        job_log_path=os.path.join(base_directory, "job_logs/")
        user_log_path=os.path.join(base_directory, "user_logs/")
        loop_until_done(createJobPar, 
                        w,
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        user_log_path=user_log_path,
                        job_log_path=job_log_path,
                        debug=debug)
    elif asset_type == "warehouse":
        warehouse_log_path=os.path.join(base_directory, "warehouse_logs/")
        user_log_path=os.path.join(base_directory, "user_logs/")
        loop_until_done(createWarehousePar, 
                        w,
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        user_log_path=user_log_path,
                        wh_log_path=warehouse_log_path,
                        debug=debug)
    elif asset_type == "query":
        warehouse_log_path=os.path.join(base_directory, "warehouse_logs/")
        loop_until_done(createQueryPar,
                        w,
                        asset_type,
                        batch_time, 
                        max_per_batch,
                        obj_count,
                        num_exec=threads, 
                        wh_log_path=warehouse_log_path,
                        debug=debug)
    elif asset_type == "dashboard":
        print("Dashboards are not currently supported, and must be manually created.")
    else:
        print("Invalid object type. Please enter one of the following: group, user, notebook, file, cluster, job, warehouse, query, dashboard")
        return


# COMMAND ----------

def update_in_batch(batch_time, clusters_per_batch, 
                    jobs_per_batch, files_per_batch, 
                    nb_per_batch, threads, 
                    base_directory="dbfs:/tmp/asset_generation",
                    num_batches=None):

    '''
    Function to batch up the updates of objects to simulate realistic usage, and reduce pressure on the workspace limits.

    Arguments:
    batch_time -- the time to wait between batches
    clusters_per_batch -- the number of clusters to update in each batch
    jobs_per_batch -- the number of jobs to update in each batch
    files_per_batch -- the number of files to update in each batch
    nb_per_batch -- the number of notebooks to update in each batch
    threads -- the number of threads to be used when creating objects
    base_directory -- [optional] the base folder in which logging folders exist
    num_batches -- [optional] the total number of batches to run; by default, run indefinitely
    '''

    # if no batch limit is provided, loop indefinitely until explicitly killed
    if not num_batches:
        num_batches = math.inf

    # initialize vars
    w = WorkspaceClient()
    debug=False
    batch_count = 0

    # if using dbfs, parse path for python os.path
    if base_directory.startswith("dbfs:"):
        check_dir = base_directory.replace("dbfs:", "/dbfs")
    else:
        check_dir = base_directory

    # validate that the base logging path exists
    if not os.path.exists(check_dir):
        print("Logging directory does not exist. Please use the logs generated by generate_in_batch.")
        return

    # validate logs exist for each object type
    user_log_path = f"{base_directory}/user_logs/"
    nb_log_path = f"{base_directory}/notebook_logs/"
    file_log_path = f"{base_directory}/file_logs/"
    cluster_log_path = f"{base_directory}/cluster_logs/"
    job_log_path = f"{base_directory}/job_logs/"

    if not os.path.exists(f"{check_dir}/user_logs/"):
        print("User logs directory does not exist. Please use the logs generated by generate_in_batch.")
        return
    elif not os.path.exists(f"{check_dir}/notebook_logs/"):
        print("Notebook logs directory does not exist. Please use the logs generated by generate_in_batch.")
        return
    elif not os.path.exists(f"{check_dir}/file_logs/"):
        print("File logs directory does not exist. Please use the logs generated by generate_in_batch.")
        return
    elif not os.path.exists(f"{check_dir}/cluster_logs/"):
        print("Cluster logs directory does not exist. Please use the logs generated by generate_in_batch.")
        return
    elif not os.path.exists(f"{check_dir}/job_logs/"):
        print("Job logs directory does not exist. Please use the logs generated by generate_in_batch.")
        return

    # run the batches
    while batch_count+1 <= num_batches:
        
        # time the batch
        start_time = time.perf_counter()

        print(f"Batch {batch_count+1} of {num_batches} started.")
        print(f"Updating notebooks...")
        updateNotebookPar(w, user_log_path, nb_log_path, nb_per_batch, threads, debug=debug)
        print(f"Updating files...")
        updateFilePar(w, user_log_path, file_log_path, files_per_batch, threads, debug=debug)
        print(f"Updating clusters...")
        updateClusterPar(w, user_log_path, cluster_log_path, clusters_per_batch, threads, debug=False)
        print(f"Updating jobs...")
        updateJobPar(w, user_log_path, job_log_path, jobs_per_batch, threads, debug=False)

        end_time = time.perf_counter()

        if end_time - start_time > batch_time*60:
            # if we have exceeded the batch time, we likely need to increase the perf characteristics
            print("WARNING: process time was greater than batch time. \nConsider increasing threads or batch_time.")
            batch_count += 1
        else:
            # otherwise, sleep for the remainder of the batch time
            if batch_count < num_batches-1:
                time_to_wait = batch_time*60 - (end_time - start_time)
                print(f"Processed batch {batch_count+1}. Waiting {round(time_to_wait)} seconds for next batch...")
                time.sleep(time_to_wait)
            batch_count += 1


# COMMAND ----------

def pull_existing_users(w=None, user_log_path="dbfs:/tmp/asset_generation/user_logs/"):

    '''
    Pull a list of existing users in the workspace and add them to the users log.

    Arguments:
    w -- a WorkspaceClient object
    user_log_path -- the path where users will be written; should match createUsersPar
    '''

    # generate a WorkspaceClient if not provided
    if not w:
        w = WorkspaceClient()

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(user_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(user_log_path)

    # get the user list and write to the table
    users = w.users.list(attributes="userName",sort_by="userName")
    user_list = [user.user_name for user in users]
    user_list.write.mode("append").format("delta").save(user_log_path)


# COMMAND ----------

def pull_existing_groups(w=None, group_log_path="dbfs:/tmp/asset_generation/group_logs/"):

    '''
    Pull a list of existing users in the workspace and add them to the users log.

    Arguments:
    w -- a WorkspaceClient object
    group_log_path -- the path where groups will be written; should match createGroupsPar
    '''

    # generate a WorkspaceClient if not provided
    if not w:
        w = WorkspaceClient()

    # create the logging path if it does not exist and write an empty delta table
    dbutils.fs.mkdirs(group_log_path)
    spark.createDataFrame([], StringType()).write.mode("append").format("delta").save(group_log_path)

    # get the user list and write to the table
    groups = w.users.list(attributes="id",sort_by="id")
    group_list = [group.id for group in groups]
    group_list.write.mode("append").format("delta").save(group_log_path)