import os
from time import sleep
import yaml
import pickle as pickle
import pandas as pd

import argo_workflows
from argo_workflows.api import workflow_service_api
from argo_workflows.model.io_argoproj_workflow_v1alpha1_workflow_create_request import \
    IoArgoprojWorkflowV1alpha1WorkflowCreateRequest

import uuid

from kube_pipe_base import VOLUME_PATH, Kube_pipe_base, kubeconfig

from minio import Minio
from minio.error import S3Error

from kubernetes import client, config
import base64



BUCKET_PATH = ".kubetmp"


def make_kube_pipeline(*args):
    return Kube_pipe(*args)

class Kube_pipe(Kube_pipe_base):

    def __init__(self,*args):
        super().__init__(*args)
        self.functionresources = {}

        config.load_kube_config()
        kubeapi = client.CoreV1Api()

        artifactsConfig = yaml.safe_load(kubeapi.read_namespaced_config_map("artifact-repositories","argo").data["default-v1"])["s3"]

        access_key = base64.b64decode(kubeapi.read_namespaced_secret(artifactsConfig["accessKeySecret"]["name"], "argo").data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        secret_key = base64.b64decode(kubeapi.read_namespaced_secret(artifactsConfig["secretKeySecret"]["name"], "argo").data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

        self.minioclient = Minio(
            "localhost:30818",
            access_key=access_key,
            secret_key=secret_key,
            secure=not artifactsConfig["insecure"])

        self.bucket = artifactsConfig["bucket"]

        found = self.minioclient.bucket_exists(self.bucket)

        if not found:
            self.minioclient.make_bucket(self.bucket)


    def config(self, resources = None,function_resources = None):
        self.kuberesources = resources
        self.functionresources = function_resources


    def uploadVariable(self, var, name):
        with open(f'/tmp/{name}.tmp', 'wb') as handle:
            pickle.dump(var, handle, protocol=pickle.HIGHEST_PROTOCOL)


        self.minioclient.fput_object(
            self.bucket, f"{name}", f'/tmp/{name}.tmp',
        )

        os.remove(f'/tmp/{name}.tmp')


    def workflow(self,X,y,funcs,name, pipeid, resources = None):

        self.uploadVariable(X,f"X{pipeid}")
        self.uploadVariable(y,f"y{pipeid}")

        workflow = {'apiVersion': 'argoproj.io/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'generateName': 'pipeline'},
                    'spec': {'entrypoint': 'pipeline-template',
                            'retryStrategy': {'limit': '4'},
                            'templates': [{'name': 'pipeline-template', 'steps': None}],
                            'ttlStrategy': {
                                            'secondsAfterSuccess': 20}}}


        templates = workflow["spec"]["templates"]
        workflow["metadata"]["generateName"] = name+str(pipeid)

        templates[0]["steps"] = []

        for i,func in enumerate(funcs):

            self.uploadVariable(func,f"func{i}{pipeid}")

            code = f"""
import pickle
import pandas

with open(\'/tmp/X\', \'rb\') as input_file:
    X = pickle.load(input_file)
    print("Loaded x")

with open(\'/tmp/y\', \'rb\') as input_file:
    y = pickle.load(input_file)
    print("Loaded y")

with open(\'/tmp/func\', \'rb\') as input_file:
    func = pickle.load(input_file)
    print("Loaded func")

print("Loaded files")

if(getattr(func,"predict",None) is None):
    X=func.fit_transform(X)

    with open('/tmp/X', \'wb\') as handle:
        pickle.dump(X, handle, protocol=pickle.HIGHEST_PROTOCOL)

else:
    model = func.fit(X,y)
    with open('/tmp/out', \'wb\') as handle:
        pickle.dump(model, handle, protocol=pickle.HIGHEST_PROTOCOL)

"""    
  
            template = {'container': 
                            {'args': [''],
                            'command': ['python', '-c'],
                            'image': 'alu0101040882/scikit:p3.6.8',
                            },
                        'inputs' : {
                            'artifacts':
                               [
                                    {"name" : f"inX{pipeid}", "path" :  "/tmp/X",    "s3": { "key" : f"X{pipeid}"}},
                                    {"name" : f"iny{pipeid}", "path" :  "/tmp/y",    "s3": { "key" : f"y{pipeid}"}},
                                    {"name" : f"func{i}{pipeid}", "path" : "/tmp/func", "s3": { "key" : f"func{i}{pipeid}"}}
                               ]
                        },
                        'outputs' : {
                            'artifacts':
                               [
                                   
                                    {"name" : f"outX{pipeid}", "path" : "/tmp/X", "archive" : {"none" : {}}, "s3": { "key" : f"X{pipeid}"}},
                               ]
                        },
                        'name': str(i) + str.lower(str(type(func).__name__))}

            #Estimator
            if(getattr(func,"predict",None) is not None):
                template["outputs"]["artifacts"] = [ {"name" : f"output{pipeid}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"output{pipeid}"}}]

            if(resources is None):
                resources = self.kuberesources

            if(self.functionresources.get(func,None) is not None):
                resources = self.functionresources.get(func)

            if(resources is not None):
                template["container"]["resources"]  = {"limits" : resources}


            template["container"]["args"][0] = code

            templates.append(template)
  
            step = [{'name': template["name"],
                    'template': template["name"]}]

            templates[0]["steps"].append(step)

         
        return self.launchFromManifest(workflow)


    def deleteTemporaryFiles(self):
        objects = self.minioclient.list_objects(self.bucket)
        for obj in objects:
            self.minioclient.remove_object(self.bucket,obj.object_name)


    def fit(self,X,y, resources = None):

        self.deleteTemporaryFiles()
        
        self.pipeIds = []
        self.models = []

        workflowNames = []

        for i , pipeline in enumerate(self.pipelines):
            self.pipeIds.append(str(uuid.uuid4())[:8])

            workflowNames.append(self.workflow(X,y,pipeline,f"{i}-{str.lower(str(type(pipeline[-1]).__name__))}-fit-",self.pipeIds[i],resources = resources))

        self.waitForWorkflows(workflowNames)

        for pipe in self.pipeIds:

            self.minioclient.fget_object(self.bucket, f"output{pipe}", f"/tmp/out{self.pipeIds[i]}.tmp")

            with open(f"/tmp/out{self.pipeIds[i]}.tmp","rb") as outfile:
                self.models.append(pickle.load(outfile))

            os.remove(f"/tmp/out{self.pipeIds[i]}.tmp")

        return self


    def score(self,X,y, pipeIndex = None):

        if self.pipelines == None or self.models == None:
            raise Exception("Model must be trained before calculating score")

        if pipeIndex == None:
            pipeIndex = range(len(self.pipelines))

        workflowNames = []

        scores = []
        
        for index in pipeIndex:
                workflowNames.append(self.workflow(X,y,self.pipelines[index][:-1],"workflow-score-",self.pipeIds[index]))

        self.waitForWorkflows(workflowNames)

        for i, index in enumerate(pipeIndex):
            
            self.minioclient.fget_object(self.bucket, f"X{self.pipeIds[index]}", f"/tmp/X{self.pipeIds[i]}.tmp")

            with open(f"/tmp/X{self.pipeIds[i]}.tmp","rb") as outfile:
                testX = pickle.load(outfile)

            os.remove(f"/tmp/X{self.pipeIds[i]}.tmp")

            self.minioclient.fget_object(self.bucket, f"X{self.pipeIds[index]}", f"/tmp/y{self.pipeIds[i]}.tmp")

            with open(f"/tmp/y{self.pipeIds[i]}.tmp","rb") as outfile:
                testy = pickle.load(outfile)

            os.remove(f"/tmp/y{self.pipeIds[i]}.tmp")

            scores.append(self.models[index].score(testX,testy))


        self.deleteTemporaryFiles()
        return scores
        



        



        
