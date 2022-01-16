import os
from argo_workflows import api
import yaml
import pandas as pd

import argo_workflows
from argo_workflows.api import workflow_service_api
from argo_workflows.model.io_argoproj_workflow_v1alpha1_workflow_create_request import \
    IoArgoprojWorkflowV1alpha1WorkflowCreateRequest

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import uuid
import glob
import functools
import atexit

import datetime
from dateutil.tz import tzutc

from argo_workflows.exceptions import NotFoundException

from time import sleep

from pprint import pprint

VOLUME_PATH = "/home/ansible/.kubetmp/"


def kubeconfig(resources):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs, resources=resources)
        return wrapper
    return decorator
        
class Kube_pipe_base:

    def __init__(self,*args):

        self.pipelines = args

        self.pipeIds = []

        self.kuberesources = None

        self.functionresources = None

        self.namespace = "argo"

        atexit.register(self.deleteTemporaryFiles)


    def make_kube_pipeline(*args):
        return Kube_pipe_base(*args)


    def config(self, resources = None,function_resources = None):
        self.kuberesources = resources
        self.functionresources = function_resources


    def launchFromManifest(self,manifest):
        api_response = self.api.create_workflow(
            namespace=self.namespace,
            body=IoArgoprojWorkflowV1alpha1WorkflowCreateRequest(workflow=manifest, _check_type=False))

        
        name = api_response["metadata"]["name"]
        print(f"Launched workflow '{name}'")
        return name


    def deleteTemporaryFiles(self):
        for pipeid in self.pipeIds:
            files = glob.glob(f"{VOLUME_PATH}*{pipeid}.tmp")
            
            for f in files:
                os.remove(f)


    def waitForWorkflows(self,workflowNames):
        
        finished = []

        while len(finished) < len(workflowNames):

            for workflowName in workflowNames:
                if(workflowName not in finished):
                    try:
                        workflow = self.api.get_workflow(namespace=self.namespace,name = workflowName)
                    except NotFoundException:
                        pass

                    status = workflow["status"]
                
                    if(getattr(status,"phase",None) is not None):

                        if(status["phase"] == "Succeeded"):
                            endtime = datetime.datetime.now(tzutc())
                            starttime = workflow["metadata"]["creation_timestamp"]

                            print(f"\nWorkflow '{workflowName}' has finished. Time ({endtime-starttime})"u'\u2713')
                            
                            finished.append(workflowName)

                        elif(status["phase"] == "Failed"):
                            self.deleteTemporaryFiles()
                            raise Exception(f"Workflow {workflowName} has failed\nStatus: {status}")

                    sleep(1)

                    print(".",end="",sep="",flush=True)

    def waitForWorkflow(self,workflowName):

        while True:
            workflow = self.api.get_workflow(namespace=self.namespace,name = workflowName)
            status = workflow["status"]
        
            if(getattr(status,"phase",None) is not None):

                if(status["phase"] == "Running"):
                    sleep(1)

                elif(status["phase"] == "Succeeded"):

                    endtime = datetime.datetime.now(tzutc())
                    starttime = workflow["metadata"]["creation_timestamp"]

                    print(f"\nWorkflow '{workflowName}' has finished. Time ({endtime-starttime})"u'\u2713')
                    return

                elif(status["phase"] == "Failed"):
                    raise Exception(f"Workflow {workflowName} has failed")


            print(".",end="",sep="",flush=True)


        



        