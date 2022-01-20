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


def make_kube_pipeline(*args):
    return Kube_pipe(*args)

class Kube_pipe():

    def __init__(self,*args, namespace = "argo", tmpFolder = "/tmp"):

        self.id = str(uuid.uuid4())[:8]
        
        self.pipelines = []
        for arg in args:
            self.pipelines.append({
                "id" : str(uuid.uuid4())[:8],
                "funcs" : arg
            })

        self.tmpFolder = tmpFolder

        self.kuberesources = None

        self.concurrent_pipelines = None

        self.function_resources = {}

        self.namespace = namespace


        config.load_kube_config()
        kubeapi = client.CoreV1Api()

        argo_ip = "https://" + kubeapi.read_namespaced_service("argo-server","argo").status.load_balancer.ingress[0].ip + ":2746"
        minio_ip  = kubeapi.read_namespaced_service("minio","argo").status.load_balancer.ingress[0].ip + ":9000"

        configuration = argo_workflows.Configuration(host=argo_ip, discard_unknown_keys=True)
        configuration.verify_ssl = False

        api_client = argo_workflows.ApiClient(configuration)
        self.api = workflow_service_api.WorkflowServiceApi(api_client)

        artifactsConfig = yaml.safe_load(kubeapi.read_namespaced_config_map("artifact-repositories","argo").data["default-v1"])["s3"]

        access_key = base64.b64decode(kubeapi.read_namespaced_secret(artifactsConfig["accessKeySecret"]["name"], "argo").data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        secret_key = base64.b64decode(kubeapi.read_namespaced_secret(artifactsConfig["secretKeySecret"]["name"], "argo").data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

        self.minioclient = Minio(
            minio_ip,
            access_key=access_key,
            secret_key=secret_key,
            secure=not artifactsConfig["insecure"]
        )

        self.bucket = artifactsConfig["bucket"]

        if not self.minioclient.bucket_exists(self.bucket):
            self.minioclient.make_bucket(self.bucket)

        atexit.register(lambda : self.deleteFiles(f"{BUCKET_PATH}/{self.id}/"))


    def config(self, resources = None,function_resources = None):
        self.kuberesources = resources
        self.function_resources = function_resources


    def uploadVariable(self, var, name, prefix = ""):
        with open(f'/tmp/{name}.tmp', 'wb') as handle:
            pickle.dump(var, handle, protocol=pickle.HIGHEST_PROTOCOL)

        if(prefix!= ""):
            prefix +="/"

        self.minioclient.fput_object(
            self.bucket, f"{BUCKET_PATH}/{self.id}/{prefix}{name}", f'{self.tmpFolder}/{name}.tmp',
        )

        os.remove(f'/tmp/{name}.tmp')


    def downloadVariable(self,name, prefix = ""):

        if(prefix!= ""):
            prefix +="/"

        self.minioclient.fget_object(self.bucket, f"{BUCKET_PATH}/{self.id}/{prefix}{name}", f"{self.tmpFolder}/{name}.tmp")

            
        with open(f"/tmp/{name}.tmp","rb") as outfile:
            var = pickle.load(outfile)

        os.remove(f"/tmp/{name}.tmp")

        return var


    def workflow(self,X,y,funcs,name, id, resources = None, fitdata = True, operation= "fit(X,y)"):
        
        self.uploadVariable(X,f"X{id}", prefix = "tmp")
        self.uploadVariable(y,f"y{id}", prefix = "tmp")

        workflow = {'apiVersion': 'argoproj.io/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'generateName': 'pipeline'},
                    'spec': {'entrypoint': 'pipeline-template',
                            'retryStrategy': {'limit': '2'},
                            'templates': [{'name': 'pipeline-template', 'steps': None}],
                            'ttlStrategy': {
                                            'secondsAfterSuccess': 20}}}


        templates = workflow["spec"]["templates"]
        workflow["metadata"]["generateName"] = name+str(id)

        templates[0]["steps"] = []

        for i,func in enumerate(funcs):
            
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

if(hasattr(func,"predict")):
    model = func.{operation}
    with open('/tmp/out', \'wb\') as handle:
        pickle.dump(model, handle, protocol=pickle.HIGHEST_PROTOCOL)

else:
    if({fitdata}):
        func=func.fit(X)
        with open('/tmp/func', \'wb\') as handle:
            pickle.dump(func, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
    X = func.transform(X)

    with open('/tmp/X', \'wb\') as handle:
        pickle.dump(X, handle, protocol=pickle.HIGHEST_PROTOCOL)

"""    

            template = {
                        'container': 
                            {'args': [''],
                            'command': ['python', '-c'],
                            'image': 'alu0101040882/scikit:p3.6.8',
                            },
                        'inputs' : {
                            'artifacts':
                               [
                                    {"name" : f"inX{id}", "path" :  "/tmp/X",    "s3":    { "key" : f"{BUCKET_PATH}/{self.id}/tmp/X{id}"}},
                                    {"name" : f"iny{id}", "path" :  "/tmp/y",    "s3":    { "key" : f"{BUCKET_PATH}/{self.id}/tmp/y{id}"}},
                                    {"name" : f"infunc{i}{id}", "path" : "/tmp/func", "s3": { "key" : f"{BUCKET_PATH}/{self.id}/func{i}{id}" if fitdata or not (hasattr(func,"predict")) else f"{BUCKET_PATH}/{self.id}/model{id}" }}
                               ]
                        },
                        'outputs' : {
                            'artifacts':
                               []
                        },
                        'name': str(i) + str.lower(str(type(func).__name__))}

            if(fitdata):
                self.uploadVariable(func,f"func{i}{id}")

            #Estimator
            if(hasattr(func,"predict")):
                template["outputs"]["artifacts"].append({"name" : f"output{id}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"{BUCKET_PATH}/{self.id}/tmp/output{id}"}})

                if(fitdata):
                    template["outputs"]["artifacts"].append({"name" : f"model{id}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"{BUCKET_PATH}/{self.id}/model{id}"}})
                
            #Transformer
            else:
                template["outputs"]["artifacts"].append({"name" : f"outX{id}", "path" : "/tmp/X", "archive" : {"none" : {}}, "s3": { "key" : f"{BUCKET_PATH}/{self.id}/tmp/X{id}"}})

                if(fitdata):
                    template["outputs"]["artifacts"].append({"name" : f"outfunc{i}{id}", "path" : "/tmp/func", "archive" : {"none" : {}}, "s3": { "key" : f"{BUCKET_PATH}/{self.id}/func{i}{id}"}})
                
            if(resources is None):
                resources = self.kuberesources

            if(self.function_resources.get(func,None) is not None):
                resources = self.function_resources.get(func)

            if(resources is not None):
                template["container"]["resources"]  = {"requests" : resources}


            template["container"]["args"][0] = code

            templates.append(template)
  
            step = [{'name': template["name"],
                    'template': template["name"]}]

            templates[0]["steps"].append(step)

   
        return self.launchFromManifest(workflow)


    def deleteFiles(self, prefix):
        objects_to_delete = self.minioclient.list_objects(self.bucket, prefix=prefix, recursive=True)
        for obj in objects_to_delete:
            self.minioclient.remove_object(self.bucket, obj.object_name)
        print(f"Artifacts deleted from {prefix}")


    def runWorkflows(self, X, y, operation, name,  fitdata, resources = None, pipeIndex = None, applyToFuncs = None, output = "output", outputPrefix = "tmp", concurrent_pipelines = None):

        if pipeIndex == None:
            pipeIndex = range(len(self.pipelines))

        
        if concurrent_pipelines == None:
            if(self.concurrent_pipelines != None):
                concurrent_pipelines = self.concurrent_pipelines
            else:
                concurrent_pipelines = len(self.pipelines)

        workflows = []

        for i , index in enumerate(pipeIndex):

            #Check that no more than "concurrent_pipelines" are running at the same time, wait for a workflow to finish
            if(len(workflows) >= concurrent_pipelines):
                finishedWorkflows = self.waitForWorkflows(workflows,numberToWait=1)
                for workflow in finishedWorkflows:
                    workflows.remove(workflow)
        
            pipeline = self.pipelines[index]

            funcs = pipeline["funcs"]

            if applyToFuncs is not None and callable(applyToFuncs):
                funcs = applyToFuncs(funcs)

            workflows.append(self.workflow(X,y, funcs, f"{i}-{str.lower(str(type( pipeline['funcs'][-1] ).__name__))}-{name}-", pipeline["id"], resources = resources, fitdata=fitdata, operation = operation))
        
        if(len(workflows) > 0):
            self.waitForWorkflows(workflows)
        
        outputs = []

        for i, index in enumerate(pipeIndex):
            outputs.append(self.downloadVariable(f"{output}{self.pipelines[index]['id']}", prefix = outputPrefix))

        return outputs



    def fit(self,X,y, resources = None, concurrent_pipelines = None):
        self.models = self.runWorkflows(X,y,"fit(X,y)", "fit", True, resources = resources, concurrent_pipelines = concurrent_pipelines)

        self.deleteFiles(f"{BUCKET_PATH}/{self.id}/tmp")

        return self
        
    def score(self,X,y, resources = None, pipeIndex = None, concurrent_pipelines = None):

        if self.pipelines == None or self.models == None:
            raise Exception("Model must be trained before calculating score")

        out =  self.runWorkflows(X,y,"score(X,y)", "score",  False,  resources = resources, pipeIndex=pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.deleteFiles(f"{BUCKET_PATH}/{self.id}/tmp")

        return out

    def transform(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):

        if self.pipelines == None or self.models == None:
            raise Exception("Transformer must be fitted before transform")

        out =  self.runWorkflows(X, None, None, "transform",  False, resources = resources, pipeIndex=pipeIndex, applyToFuncs= lambda f : f[:-1], output = "X",concurrent_pipelines = concurrent_pipelines)

        self.deleteFiles(f"{BUCKET_PATH}/{self.id}/tmp")

        return out


    def predict(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        if self.pipelines == None or self.models == None:
            raise Exception("Model must be trained before calculating predict")

        out =  self.runWorkflows(X, None, "predict(X)", "predict", False, resources = resources, pipeIndex = pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.deleteFiles(f"{BUCKET_PATH}/{self.id}/tmp")

        return out
        
    def fit_predict(self, X, y, resources = None, pipeIndex = None, concurrent_pipelines = None):

        out = self.runWorkflows(X, y, "fit_predict(X,y)", "fit_predict", True, resources = resources, pipeIndex = pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.deleteFiles(f"{BUCKET_PATH}/{self.id}/tmp")

        return out


    def config(self, resources = None,function_resources = None, concurrent_pipelines = None):
        self.kuberesources = resources
        self.function_resources = function_resources
        self.concurrent_pipelines = concurrent_pipelines


    def launchFromManifest(self,manifest):
        api_response = self.api.create_workflow(
            namespace=self.namespace,
            body=IoArgoprojWorkflowV1alpha1WorkflowCreateRequest(workflow=manifest, _check_type=False))

        name = api_response["metadata"]["name"]
        print(f"Launched workflow '{name}'")
        return name


    def waitForWorkflows(self,workflowNames, numberToWait = None):

        if(numberToWait == None):
            numberToWait = len(workflowNames)    
        
        finished = []

        while len(finished) < numberToWait:

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
                            self.deleteFiles(workflowName)
                            
                            finished.append(workflowName)

                        elif(status["phase"] == "Failed"):
                            self.deleteFiles(f"{BUCKET_PATH}/{self.id}/")
                            raise Exception(f"Workflow {workflowName} has failed")

                    sleep(1)

                    print(".",end="",sep="",flush=True)

        return finished


    def getWorkflowStatus(self, workflowName):
        try:
            workflow = self.api.get_workflow(namespace=self.namespace,name = workflowName)
            return workflow["status"]

        except NotFoundException:
            return None



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




        
