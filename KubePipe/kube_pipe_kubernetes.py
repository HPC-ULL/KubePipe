import os
from time import sleep
import yaml
import cloudpickle as pickle

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

from .Kube_pipe import Kube_pipe_base

from argo_workflows.exceptions import NotFoundException


BUCKET_PATH = ".kubetmp"

def make_kube_pipeline(*args, **kwargs):
    return Kube_pipe(*args, **kwargs)

class Kube_pipe(Kube_pipe_base):

    def __init__(self,*args, argo_ip = None, minio_ip = None, access_key = None, secret_key = None):
        super().__init__(*args, argo_ip = argo_ip, minio_ip = minio_ip, access_key = access_key, secret_key = secret_key)

        
    def workflow(self, funcs,name, pipeId, operation= "fit(X,y)"):


        self.uploadVariable(funcs,"funcs","tmp")

        code = f"""
import cloudpickle as pickle

from sklearn.pipeline import make_pipeline

from minio import Minio

import os

minioclient = Minio(
            'minio:9000',
            access_key='{self.access_key}',
            secret_key='{self.secret_key}',
            secure=False
)


minioclient.fget_object('{self.bucket}', '{BUCKET_PATH}/{self.id}/tmp/X', '/tmp/X')
with open(\'/tmp/X\', \'rb\') as input_file:
    X = pickle.load(input_file)
    print("Loaded x")
os.remove('/tmp/X')

minioclient.fget_object('{self.bucket}', '{BUCKET_PATH}/{self.id}/tmp/y', '/tmp/y')
with open(\'/tmp/y\', \'rb\') as input_file:
    y = pickle.load(input_file)
    print("Loaded y")
os.remove('/tmp/y')


minioclient.fget_object('{self.bucket}', '{BUCKET_PATH}/{self.id}/tmp/funcs', '/tmp/funcs')
with open(\'/tmp/funcs\', \'rb\') as input_file:
    funcs = pickle.load(input_file)
    print("Loaded func")
os.remove('/tmp/funcs')

pipe = make_pipeline(*funcs)

output = pipe.{operation}

with open('/tmp/out', \'wb\') as handle:
    pickle.dump(output, handle)


minioclient.fput_object(
            '{self.bucket}', '{BUCKET_PATH}/{self.id}/{pipeId}', '/tmp/out',
)

print('Output exported to {BUCKET_PATH}/{self.id}/{pipeId}' )

"""    
        command = ["python3" ,"-c", code]
        container = client.V1Container(
            name=f"{pipeId}",
            image="alu0101040882/kubepipe:p3.7.3-minio",
            command=command
            
        )

        spec=client.V1PodSpec(restart_policy="Never", containers=[container])

        workflowname = f"pipeline-{name}-{self.id}-{pipeId}"
        body = client.V1Job(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=workflowname),
            spec=spec
            )

        api_response = self.kubeApi.create_namespaced_pod(
        body=body,
        namespace=self.namespace)
        print("\nLanzado el pipeline: '" + workflowname + "'")
        return workflowname


    def runPipelines(self, X, y, operation, name, resources = None, pipeIndex = None, applyToFuncs = None, output = "output", outputPrefix = "tmp", concurrent_pipelines = None):

        if pipeIndex == None:
            pipeIndex = range(len(self.pipelines))

        
        if concurrent_pipelines == None:
            if(self.concurrent_pipelines != None):
                concurrent_pipelines = self.concurrent_pipelines
            else:
                concurrent_pipelines = len(self.pipelines)

        workflows = []


        self.uploadVariable(X,f"X", prefix = "tmp")
        self.uploadVariable(y,f"y", prefix = "tmp")


        for i , index in enumerate(pipeIndex):

            #Check that no more than "concurrent_pipelines" are running at the same time, wait for a workflow to finish
            if(len(workflows) >= concurrent_pipelines):
                finishedWorkflows = self.waitForPipelines(workflows,numberToWait=1)
                for workflow in finishedWorkflows:
                    workflows.remove(workflow)
        
            pipeline = self.pipelines[index]

            funcs = pipeline["funcs"]

            if applyToFuncs is not None and callable(applyToFuncs):
                funcs = applyToFuncs(funcs)

            workflows.append(self.workflow( funcs, f"{i}-{str.lower(str(type( pipeline['funcs'][-1] ).__name__))}-{name}-", pipeline["id"], operation = operation))
        
        if(len(workflows) > 0):
            self.waitForPipelines(workflows)
        
        outputs = []

        for i, index in enumerate(pipeIndex):
            outputs.append(self.downloadVariable(f"{self.pipelines[index]['id']}"))

        return outputs


    def fit(self,X,y, resources = None, concurrent_pipelines = None):
        output = self.runPipelines(X,y,"fit(X,y)", "fit", resources = resources, concurrent_pipelines = concurrent_pipelines)

        self.deleteFiles(f"{BUCKET_PATH}/{self.id}/tmp")

        return output
        

    def config(self, resources = None,  concurrent_pipelines = None, namespace = None, tmpFolder = None):
        
        if namespace: self.namespace = namespace
        if tmpFolder: self.tmpFolder = tmpFolder
        if resources: self.kuberesources = resources 
        if concurrent_pipelines: self.concurrent_pipelines = concurrent_pipelines



    def waitForPipelines(self,workflowNames, numberToWait = None):

        if(numberToWait == None):
            numberToWait = len(workflowNames)    
        
        finished = []

        while len(finished) < numberToWait:

            for workflowName in workflowNames:
                if(workflowName not in finished):
                    workflow = None

                    try:
                        workflow = self.kubeApi.read_namespaced_pod_status(
                                name =  workflowName,
                                namespace=self.namespace)

                        status = workflow.status.phase
                    
                        if(status == "Succeeded"):

                            print(f"\nWorkflow '{workflowName}' has finished."u'\u2713')
                             
                            api_response = self.kubeApi.delete_namespaced_pod(workflowName, self.namespace)
                            
                            
                            finished.append(workflowName)

                        elif(status == "Failed" or status == "Error"):
                            
                            raise Exception(f"Workflow {workflowName} has failed")
                       

                    except Exception as e:
                        if(e.__class__.__name__ == "ApiException" and e.status == 404):
                            None
                        else:
                            raise e
                    
              
    
            sleep(1)

            print(".",end="",sep="",flush=True)

        return finished


    