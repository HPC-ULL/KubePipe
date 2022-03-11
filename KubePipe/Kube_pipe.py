import os
from time import sleep
import yaml
import pickle as pickle

import argo_workflows
from argo_workflows.api import workflow_service_api
from argo_workflows.model.io_argoproj_workflow_v1alpha1_workflow_create_request import \
    IoArgoprojWorkflowV1alpha1WorkflowCreateRequest

import uuid

from minio import Minio

from kubernetes import client, config
import base64

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import uuid
import atexit

import datetime
from dateutil.tz import tzutc

from argo_workflows.exceptions import NotFoundException


BUCKET_PATH = ".kubetmp"


class Kube_pipe():

    def __init__(self,*args, argo_ip = None, minio_ip = None, access_key = None, secret_key = None):

        self.id = str(uuid.uuid4())[:8]
          
        self.pipelines = []
        for arg in args:
            self.pipelines.append({
                "id" : str(uuid.uuid4())[:10],
                "funcs" : arg
            })

        self.tmpFolder = "/tmp"

        self.kuberesources = None

        self.concurrent_pipelines = None

        self.namespace = "argo"

        self.models = None

        config.load_kube_config()
        self.kubeApi = client.CoreV1Api()

        if not minio_ip: minio_ip  = self.kubeApi.read_namespaced_service("minio",self.namespace).status.load_balancer.ingress[0].ip + ":9000"

        configuration = argo_workflows.Configuration(host=argo_ip, discard_unknown_keys=True)
        configuration.verify_ssl = False

        artifactsConfig = yaml.safe_load(self.kubeApi.read_namespaced_config_map("artifact-repositories","argo").data["default-v1"])["s3"]

        if not access_key: access_key = base64.b64decode(self.kubeApi.read_namespaced_secret(artifactsConfig["accessKeySecret"]["name"], self.namespace).data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        if not secret_key: secret_key = base64.b64decode(self.kubeApi.read_namespaced_secret(artifactsConfig["secretKeySecret"]["name"], self.namespace).data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

        self.minioclient = Minio(
            minio_ip,
            access_key=access_key,
            secret_key=secret_key,
            secure=not artifactsConfig["insecure"]
        )

        self.access_key = access_key
        self.secret_key = secret_key

        self.bucket = artifactsConfig["bucket"]

        if not self.minioclient.bucket_exists(self.bucket):
            self.minioclient.make_bucket(self.bucket)

        atexit.register(lambda : self.deleteFiles(f"{BUCKET_PATH}/{self.id}/"))





    def uploadVariable(self, var, name, prefix = ""):
        with open(f'{self.tmp_folder}/{name}.tmp', 'wb') as handle:
            pickle.dump(var, handle, protocol=pickle.HIGHEST_PROTOCOL)

        if(prefix!= ""):
            prefix +="/"

        self.minioclient.fput_object(
            self.bucket, f"{BUCKET_PATH}/{self.id}/{prefix}{name}", f'{self.tmp_folder}/{name}.tmp',
        )

        os.remove(f'{self.tmp_folder}/{name}.tmp')


    def downloadVariable(self,name, prefix = ""):

        if(prefix!= ""):
            prefix +="/"

        self.minioclient.fget_object(self.bucket, f"{BUCKET_PATH}/{self.id}/{prefix}{name}", f"{self.tmp_folder}/{name}.tmp")

            
        with open(f"{self.tmp_folder}/{name}.tmp","rb") as outfile:
            var = pickle.load(outfile)

        os.remove(f"{self.tmp_folder}/{name}.tmp")

        return var


   
    def deleteFiles(self, prefix):
        objects_to_delete = self.minioclient.list_objects(self.bucket, prefix=prefix, recursive=True)
        for obj in objects_to_delete:
            self.minioclient.remove_object(self.bucket, obj.object_name)
        print(f"Artifacts deleted from {prefix}")



    def config(self, resources = None, function_resources = None, concurrent_pipelines = None, namespace = None, tmp_folder = None):
        
        if namespace: self.namespace = namespace
        if tmp_folder: self.tmp_folder = tmp_folder
        if resources: self.kuberesources = resources 
        if function_resources: self.function_resources = function_resources
        if concurrent_pipelines: self.concurrent_pipelines = concurrent_pipelines



