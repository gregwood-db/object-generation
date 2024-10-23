# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import io
import time
import os
import uuid
import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ListClustersFilterBy, State, ClusterSpec
from databricks.sdk.service import jobs, sql, workspace
from databricks.sdk.service.iam import ComplexValue

# COMMAND ----------

def createGroup(w):

    """
    Create a new group.

    Arguments:
    w -- a WorkspaceClient object

    Returns:
    group_id -- the id of the new group
    """

    # generate a random group name
    group_name = str(uuid.uuid4())

    group = w.groups.create(display_name=group_name)
    return group.id

# COMMAND ----------

def createUser(w, group):

    """
    Create a new user in the workspace with a random username.

    Arguments:
    w -- a WorkspaceClient object

    Returns:
    user_id -- the email address of the new user
    """

    user_name = str(uuid.uuid4())
    user_id = f"{user_name}@databricks.com"
    group_obj = group_val = ComplexValue(value=group)
    try:
        user = w.users.create(display_name=user_name, user_name=user_id, groups=[group_obj])
    except Exception:
        return None
    return user.user_name
    

# COMMAND ----------

def createOrUpdateNotebook(w, size, owner=None, path=None):

    """
    Create or update a notebook that belongs to a user specified by owner.

    Note that only one of owner or path may be specified. If both are
    specified, path is disregarded and a new file is created.

    Arguments:
    w -- a WorkspaceClient object
    owner -- the email address of the notebook owner
    size -- the size of the notebook, in bytes
    path -- the path to an existing notebook to update; this will create a new
            notebook and overwrite the existing notebook.

    Returns:
    path -- the path of the created notebook
    """

    nb_id = str(uuid.uuid4())
    if not path and not owner:
        raise Exception("Either owner or path must be specified when creating a file.")
    elif not path:
        path = f"/Users/{owner}/{nb_id}.py"
        
    w.workspace.upload(path, io.BytesIO(os.urandom(size)), overwrite=True)

    return path
    

# COMMAND ----------

def createOrUpdateFile(w, size, owner=None, path=None):

    """
    Create or update a file that belongs to a user specified by owner.

    Note that only one of owner or path may be specified. If both are
    specified, path is disregarded and a new file is created.

    Arguments:
    w -- a WorkspaceClient object
    size -- the size of the file, in bytes
    owner -- the email address of the file owner
    path -- the path to an existing file to update; this will create a new
            file and overwrite the existing file

    Returns:
    path -- the path of the created file
    """

    file_name = str(uuid.uuid4())
    if not path and not owner:
        raise Exception("Either owner or path must be specified when creating a file.")
    elif not path:
        path = f"/Users/{owner}/{file_name}"
       
    w.workspace.upload(path=path, 
                       content=io.BytesIO(os.urandom(size)), 
                       format=workspace.ImportFormat.AUTO, 
                       overwrite=True)

    return path
    

# COMMAND ----------

def createOrUpdateCluster(w, policy, owner = None, cluster_id = None):
    
    """
    Create a new cluster in the workspace, or update an existing cluster's owner.

    Arguments:
    w -- a WorkspaceClient object
    policy -- a cluster_policy ID to apply to this cluster
    owner -- [optional] required if cluster_id is provided; the email address of the user 
              to set as the cluster owner. If provided without a cluster_id, set
              the new cluster's owner to this user
    cluster_id -- [optional] the id of the cluster to update

    Returns:
    cluster_id -- the id of the cluster that was created or updated
    """

    # if cluster_id is provided, update the cluster with a new owner
    if cluster_id:
        if not owner:
            raise ValueError("owner is required if cluster_id is provided")
        else:
            w.clusters.change_owner(cluster_id=cluster_id, owner_username=owner)
            return cluster_id

    # if no cluster_id is provided, create a new cluster with latest dbr and random name
    cluster_name = f"{str(uuid.uuid4())}"
    latest = w.clusters.select_spark_version(latest=True, long_term_support=True)

    # create the cluster; do not wait for state transition
    try:
        cluster = w.clusters.create_and_wait(cluster_name=cluster_name,
                                            node_type_id="m5d.large",
                                            spark_version=latest,
                                            autotermination_minutes=10,
                                            num_workers=1,
                                            apply_policy_default_values=True,
                                            policy_id=policy,
                                            timeout=datetime.timedelta(seconds=0))
    except TimeoutError:
        pass
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

    # get the cluster_id of the newly-created cluster
    cluster_filter = ListClustersFilterBy([State.RUNNING, State.PENDING])
    clusters = w.clusters.list(filter_by=cluster_filter)
    cluster_id = [c for c in clusters if c.cluster_name == cluster_name][0].cluster_id

    # terminate the cluster to avoid unnecessary DBU usage; do not wait for state transition
    try:
        _ = w.clusters.delete_and_wait(cluster_id=cluster_id,
                                    timeout=datetime.timedelta(seconds=0)).result()
    except TimeoutError:
        pass
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

    return cluster_id


# COMMAND ----------

def createClusterPolicy(w):

    policy_name = str(uuid.uuid4())

    # create a basic cluster policy and return the object
    policy = w.cluster_policies.create(name=policy_name,
                                       definition="""{
                                           "spark_conf.spark.databricks.delta.preview.enabled": {
                                               "type": "fixed",
                                               "value": "true"}
                                               }""")
    
    return policy

# COMMAND ----------

def createJob(w, owner):
    
    """
    Create a job that belongs to a user specified by owner.

    Arguments:
    w -- a WorkspaceClient object
    owner -- the email address of the job owner

    Returns:
    job_id -- the id of the job that was created
    """

    # uuid for job name
    job_id = str(uuid.uuid4())

    # create a new notebook for this job
    nb_path = createOrUpdateNotebook(w, 4096, owner)
    
    # create a cluster definition for the job
    latest = w.clusters.select_spark_version(latest=True, long_term_support=True)
    cluster_spec = ClusterSpec(node_type_id="m5d.large", spark_version=latest, num_workers=1)
    job_cluster = jobs.JobCluster(job_cluster_key="test_load", new_cluster=cluster_spec)

    # define notebook tasks
    first_task = jobs.Task(
        description="loadtest",
        notebook_task=jobs.NotebookTask(notebook_path=nb_path),
        job_cluster_key="test_load",
        task_key="task1",
        timeout_seconds=0)

    second_task = jobs.Task(
        description="loadtest",
        notebook_task=jobs.NotebookTask(notebook_path=nb_path),
        job_cluster_key="test_load",
        task_key="task2",
        depends_on=[jobs.TaskDependency(task_key="task1")],
        timeout_seconds=0)

    # define an ACL for the job
    acl = jobs.JobAccessControlRequest(user_name=owner, permission_level=jobs.JobPermissionLevel.IS_OWNER)

    # create email notif object
    email_obj = jobs.JobEmailNotifications(on_failure=[owner])

    # create the job and return the job object
    created_job = w.jobs.create(name=job_id,
                                job_clusters=[job_cluster],
                                access_control_list=[acl],
                                tasks=[first_task, second_task],
                                email_notifications=email_obj)
    
    return created_job.job_id

# COMMAND ----------

def updateJob(w, job_id, email):
    
    """
    Update an existing job that with a new user, who will gain CAN_MANAGE permissions.

    Arguments:
    w -- a WorkspaceClient object
    job_id -- the job id of the existing job to be updated
    email -- the email address of the job manager to be added
    """

    # define the updated job ACL
    acl = jobs.JobAccessControlRequest(user_name=email, permission_level=jobs.JobPermissionLevel.CAN_MANAGE)

    # update the acl
    updated_job = w.jobs.update_permissions(job_id=job_id, access_control_list=[acl])

# COMMAND ----------

def createWarehouse(w, owner):
    
    """
    Create a new SQL Warehouse.

    Arguments:
    w -- a WorkspaceClient object
    owner -- the email address of the warehouse owner

    Returns:
    wh_id -- the Id of the created warehouse
    """

    # uuid for warehouse name
    wh_id = str(uuid.uuid4())

    # create the warehouse without waiting for status update
    try:
        wh = w.warehouses.create_and_wait(
            name=wh_id,
            cluster_size="2X-Small",
            max_num_clusters=1,
            auto_stop_mins=10,
            tags=sql.EndpointTags(
                custom_tags=[sql.EndpointTagPair(key="Owner", 
                                                value=owner)
                            ]),
            timeout=datetime.timedelta(seconds=0))
    except TimeoutError:
        pass
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

    # get the warehouse_id of the newly-created warehouse
    wh_list = w.warehouses.list()
    wh_id = [w for w in wh_list if w.name == wh_id][0].id

    # terminate the warehouse to avoid unnecessary DBU usage; do not wait for state transition
    try:
        _ = w.warehouses.stop_and_wait(id=wh_id,
                                    timeout=datetime.timedelta(seconds=0)).result()
    except TimeoutError:
        pass
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

    # set the owner
    wh_acl = sql.WarehouseAccessControlRequest(user_name=owner, permission_level=sql.WarehousePermissionLevel.IS_OWNER)
    w.warehouses.set_permissions(warehouse_id=wh_id, access_control_list=[wh_acl])

    return wh_id


# COMMAND ----------

def createQuery(w, wh_id):

    """
    Create a new SQL Query.

    Arguments:
    w -- a WorkspaceClient object
    wh_id -- the warehouse id that the query is tied to
    """

    # generate a query uuid
    query_id = str(uuid.uuid4())

    # create the query
    query = w.queries.create(query=sql.CreateQueryRequestQuery(display_name=query_id,
                                                               warehouse_id=wh_id,
                                                               description="load testing query",
                                                               query_text="SHOW TABLES"))
    
    return query.id

# COMMAND ----------

def createDashboard(w):

    # create an empty dashboard; this is all that is supported via API
    dash_id = str(uuid.uuid4())
    dash = w.dashboards.create(name=dash_id)