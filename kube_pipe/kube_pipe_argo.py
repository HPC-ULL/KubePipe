import os
from time import sleep
import yaml
import dill as pickle

import argo_workflows
from argo_workflows.api import workflow_service_api
from argo_workflows.model.io_argoproj_workflow_v1alpha1_workflow_create_request import \
    IoArgoprojWorkflowV1alpha1WorkflowCreateRequest

from pathlib import Path

from minio import Minio

import base64

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import uuid


import datetime
from dateutil.tz import tzutc

from argo_workflows.exceptions import NotFoundException

from .kube_pipe_base import KubePipeBase


class KubePipeArgo(KubePipeBase):

    def __init__(self,*args, argo_ip = None, minio_ip = None, access_key = None, secret_key = None, registry_ip = None, minio_bucket_path = ".kubetmp", tmpFolder ="/tmp", namespace = "argo", use_gpu = False,images = None):

        super().__init__(*args, images = images, registry_ip = registry_ip, use_gpu = False)
        self.tmpFolder = tmpFolder
        self.namespace = namespace

        self.backend = "argo"

        Path(tmpFolder).mkdir(parents=True, exist_ok=True)

        self.minio_bucket_path = minio_bucket_path

        self.function_resources = {}

        if not argo_ip: argo_ip = "https://" + self.kubeapi.read_namespaced_service("argo-server","argo").status.load_balancer.ingress[0].ip + ":2746"
        if not minio_ip: minio_ip  = self.kubeapi.read_namespaced_service("minio","argo").status.load_balancer.ingress[0].ip + ":9000"

        configuration = argo_workflows.Configuration(host=argo_ip, discard_unknown_keys=True)
        configuration.verify_ssl = False

        api_client = argo_workflows.ApiClient(configuration)
        self.api = workflow_service_api.WorkflowServiceApi(api_client)

        artifactsConfig = yaml.safe_load(self.kubeapi.read_namespaced_config_map("artifact-repositories","argo").data["default-v1"])["s3"]

        if not access_key: access_key = base64.b64decode(self.kubeapi.read_namespaced_secret(artifactsConfig["accessKeySecret"]["name"], "argo").data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        if not secret_key: secret_key = base64.b64decode(self.kubeapi.read_namespaced_secret(artifactsConfig["secretKeySecret"]["name"], "argo").data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

        self.minioclient = Minio(
            minio_ip,
            access_key=access_key,
            secret_key=secret_key,
            secure=not artifactsConfig["insecure"]
        )

        self.node_selector = None
        self.bucket = artifactsConfig["bucket"]

        if not self.minioclient.bucket_exists(self.bucket):
            self.minioclient.make_bucket(self.bucket)



    def clean_workflows(self):
        self.delete_files(f"{self.minio_bucket_path}/{self.id}/")



    def workflow(self,funcs,name, pipeId, resources = None, fit_data = True, operation= "fit(X,y)", measure_energy = False, node_selector = None, additional_args = None, image = "python"):

        if(node_selector == None):
            node_selector = self.node_selector


        workflow = {'apiVersion': 'argoproj.io/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'generateName': 'pipeline'},
                    'spec': {'entrypoint': 'pipeline-template',
                            'retryStrategy': {'limit': '2'},
                            'templates': [{'name': 'pipeline-template', 'steps': None}],
                            'ttlStrategy': {
                                            'secondsAfterSuccess': 20},
                                            
                                }}


        if(measure_energy):
            workflow["spec"]["volumes"] = [{
                        "name" : "vol",
                        "hostPath" :{
                            "path" : "/dev/cpu"
                        }}
                            ] 
                

        templates = workflow["spec"]["templates"]
        workflow["metadata"]["generateName"] = name+str(pipeId)

        templates[0]["steps"] = []

        for i , func in enumerate(funcs):
            func_name = str(type(func).__name__).lower()
            func_add_args = {}
            for key, arg in additional_args.items():
                split = key.split("__")
                if(len(split)>1):
                    key, arg_name = split[0], split[1]
                    if(key == func_name):
                        func_add_args[arg_name] = arg
            
            code = f"""
import dill as pickle
import sys

def work():
    with open(\'/tmp/X\', \'rb\') as input_file:
        X = pickle.load(input_file)
        print("Loaded x")

    with open(\'/tmp/y\', \'rb\') as input_file:
        y = pickle.load(input_file)
        print("Loaded y")

    with open(\'/tmp/func\', \'rb\') as input_file:
        func = pickle.load(input_file)
        print("Loaded func")

    if({bool(func_add_args)}):
        with open(\'/tmp/add_args\', \'rb\') as input_file:
            add_args= pickle.load(input_file)
            print("Loaded add_args")
    else:
        add_args = dict()


    print("Loaded files")

    if(hasattr(func,"predict")):
        output = func.{operation}
        try:
            from scikeras.wrappers import BaseWrapper
            if(isinstance(output,BaseWrapper)):
                output.model_.save('/tmp/out',save_format="h5")
            else:
                with open('/tmp/out', \'wb\') as handle:
                    pickle.dump(output, handle)
        except ModuleNotFoundError:
            with open('/tmp/out', \'wb\') as handle:
                pickle.dump(output, handle)

    else:
        if({fit_data}):
            func=func.fit(X)
            with open('/tmp/func', \'wb\') as handle:
                pickle.dump(func, handle)
        
        X = func.transform(X)

        with open('/tmp/X', \'wb\') as handle:
            pickle.dump(X, handle)

if({measure_energy}):
    from pyeml import measure_function
    energy = measure_function(work)[1]

    with open('/tmp/energy', \'wb\') as handle:
        pickle.dump(energy, handle)

else:
    work()
"""    

            template = {

                        'container': 
                            {'args': [''],
                            'command': ['python', '-c'],
                            'image': image,
                            },
                        'inputs' : {
                            'artifacts':
                               [
                                    {"name" : f"inX{pipeId}", "path" :  "/tmp/X",    "s3":    { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/X{pipeId}" if i >= 1 else f"{self.minio_bucket_path}/{self.id}/tmp/X" }} ,
                                    {"name" : f"iny{pipeId}", "path" :  "/tmp/y",    "s3":    {  "key" : f"{self.minio_bucket_path}/{self.id}/tmp/y" }},
                                    {"name" : f"infunc{i}{pipeId}", "path" : "/tmp/func", "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/func{id(func)}{pipeId}" if fit_data or not (hasattr(func,"predict")) else f"{self.minio_bucket_path}/{self.id}/model{pipeId}" }}
                               ]
                        },
                        'outputs' : {
                            'artifacts':
                               []
                        },
                        'name': str(i) + str.lower(str(type(func).__name__))}

            if(func_add_args):
                template["inputs"]["artifacts"].append({"name" : f"add_args{i}{pipeId}", "path" : "/tmp/add_args", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/add_args{id(func)}{pipeId}"}})
                self.upload_variable(func_add_args,f"add_args{id(func)}{pipeId}", prefix = "tmp")



            if(node_selector is not None):
                template["nodeSelector"] = node_selector
            
            if(measure_energy):
                template["container"]["volumeMounts"] = [{
                    "name" : "vol",
                    "mountPath" : "/dev/cpu"
                }]

                template["container"]["securityContext"] = {
                    "privileged" : True
                }

                print("output key",  f"{self.minio_bucket_path}/{self.id}/tmp/energy{i}{pipeId}")
                template["outputs"]["artifacts"].append({"name" : f"energy{i}{pipeId}", "path" : "/tmp/energy", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/energy{i}{pipeId}"}})


            if(fit_data):
                self.upload_variable(func,f"func{id(func)}{pipeId}")

            #Estimator
            if(hasattr(func,"predict")):
                template["outputs"]["artifacts"].append({"name" : f"output{pipeId}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/output{pipeId}"}})

                if(fit_data):
                    template["outputs"]["artifacts"].append({"name" : f"model{pipeId}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/model{pipeId}"}})
                
            #Transformer
            else:
                template["outputs"]["artifacts"].append({"name" : f"outX{pipeId}", "path" : "/tmp/X", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/X{pipeId}"}})

                if(fit_data):
                    template["outputs"]["artifacts"].append({"name" : f"outfunc{i}{pipeId}", "path" : "/tmp/func", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/func{id(func)}{pipeId}"}})
            
            
            if(resources is None):
                resources = self.kuberesources

            for clase, config in self.function_resources.items():
                if(isinstance(func,clase)):
                    resources = config


            if(resources is not None):
                template["container"]["resources"]  = {"limits" : resources}

            template["container"]["args"][0] = code

            templates.append(template)
  
            step = [{'name': template["name"],
                    'template': template["name"]}]

            templates[0]["steps"].append(step)

            api_response = self.api.create_workflow(
                namespace=self.namespace,
                body=IoArgoprojWorkflowV1alpha1WorkflowCreateRequest(workflow=workflow, _check_type=False
            ))

            name = api_response["metadata"]["name"]

        return name


    def delete_files(self, prefix):
        objects_to_delete = self.minioclient.list_objects(self.bucket, prefix=prefix, recursive=True)
        for obj in objects_to_delete:
            self.minioclient.remove_object(self.bucket, obj.object_name)
        print(f"Artifacts deleted from {prefix}")


    def get_models(self):
        output = []

        for pipeline in self.pipelines:
            output.append(self.download_variable(f"model{pipeline['id']}" ))

        return output
  


    def config(self, resources = None, function_resources = None, concurrent_pipelines = None, namespace = None, tmpFolder = None,node_selector = None):
        if node_selector: self.node_selector = node_selector
        if namespace: self.namespace = namespace
        if tmpFolder: self.tmpFolder = tmpFolder
        if resources: self.kuberesources = resources 
        if function_resources: self.function_resources = function_resources
        if concurrent_pipelines: self.concurrent_pipelines = concurrent_pipelines


    def get_energy(self,pipeIndex):
        energy = []

        for i, index in enumerate(pipeIndex):
            energy.append({})
            for j, func in enumerate(self.pipelines[index]["funcs"]):
                func_energy = self.download_variable(f"energy{j}{self.pipelines[index]['id']}", prefix = "tmp")
                if(j == 0):
                    energy[-1] = func_energy
                else:
                    for device, consumed in func_energy.items():
                        energy[-1][device] += consumed
        return energy



    def is_workflow_finished(self, workflow_name):

        try:
            workflow = self.api.get_workflow(namespace=self.namespace,name = workflow_name)

        except NotFoundException:
                print(f"\nWorkflow '{workflow_name}' has been deleted.")
                return True
            
        if(workflow is not None):
            status = workflow["status"]
        
            if(getattr(status,"phase",None) is not None):

                if(status["phase"] == "Succeeded"):
                    return True

                elif(status["phase"] == "Failed"):
                    self.delete_files(f"{self.minio_bucket_path}/{self.id}/")
                    raise Exception(f"Workflow {workflow_name} has failed")
        return False

    