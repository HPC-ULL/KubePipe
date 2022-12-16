import os
from time import sleep
import yaml
import dill as pickle

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

from .kube_pipe_base import KubePipeBase


class KubePipeArgo(KubePipeBase):

    def __init__(self,*args, argo_ip = None, minio_ip = None, access_key = None, secret_key = None, minio_bucket_path = ".kubetmp", tmpFolder ="/tmp", namespace = "argo"):

        super().__init__(*args)
        self.tmpFolder = tmpFolder
        self.namespace = namespace

        self.minio_bucket_path = minio_bucket_path

        self.function_resources = {}

        config.load_kube_config()
        kubeapi = client.CoreV1Api()

        if not argo_ip: argo_ip = "https://" + kubeapi.read_namespaced_service("argo-server","argo").status.load_balancer.ingress[0].ip + ":2746"
        if not minio_ip: minio_ip  = kubeapi.read_namespaced_service("minio","argo").status.load_balancer.ingress[0].ip + ":9000"

        configuration = argo_workflows.Configuration(host=argo_ip, discard_unknown_keys=True)
        configuration.verify_ssl = False

        api_client = argo_workflows.ApiClient(configuration)
        self.api = workflow_service_api.WorkflowServiceApi(api_client)

        artifactsConfig = yaml.safe_load(kubeapi.read_namespaced_config_map("artifact-repositories","argo").data["default-v1"])["s3"]

        if not access_key: access_key = base64.b64decode(kubeapi.read_namespaced_secret(artifactsConfig["accessKeySecret"]["name"], "argo").data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        if not secret_key: secret_key = base64.b64decode(kubeapi.read_namespaced_secret(artifactsConfig["secretKeySecret"]["name"], "argo").data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

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


    def upload_variable(self, var, name, prefix = ""):
        with open(f'{self.tmpFolder}/{name}.tmp', 'wb') as handle:
            pickle.dump(var, handle, byref=False)

        if(prefix!= ""):
            prefix +="/"

        up = self.minioclient.fput_object(
            self.bucket, f"{self.minio_bucket_path}/{self.id}/{prefix}{name}", f'{self.tmpFolder}/{name}.tmp',
        )
        os.remove(f'{self.tmpFolder}/{name}.tmp')


    def download_variable(self,name, prefix = "", delete = False):
        if(prefix!= "" and prefix[-1] != "/"):
            prefix +="/"

        self.minioclient.fget_object(self.bucket, f"{self.minio_bucket_path}/{self.id}/{prefix}{name}", f"{self.tmpFolder}/{name}.tmp")

        try:
            with open(f"{self.tmpFolder}/{name}.tmp","rb") as outfile:
                var = pickle.load(outfile)
        except Exception as e:
            from tensorflow.keras.models import load_model
            var = load_model(f"{self.tmpFolder}/{name}.tmp",compile=False)

        os.remove(f"{self.tmpFolder}/{name}.tmp")

        if(delete):
            self.minioclient.remove_object(self.bucket, self.bucket, f"{self.minio_bucket_path}/{self.id}/{prefix}{name}")


        return var


    def workflow(self,X,y,funcs,name, pipeId, resources = None, fitdata = True, operation= "fit(X,y)", measure_energy = False, node_selector = None, additional_args = None):


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

        for i,func in enumerate(funcs):
            func_name = func.__name__.lower()
            func_add_args = {}
            for key, arg in additional_args.items():
                split = key.split("__")
                if(len(split)>1):
                    key, arg_name = split[0], split[1]
                    if(key == func_name):
                        func_add_args[arg_name] = arg
            
            print(func_name, func_add_args.keys())
            code = f"""
import dill as pickle

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

        from scikeras.wrappers import BaseWrapper
        if(isinstance(output,BaseWrapper)):
            output.model_.save('/tmp/out',save_format="h5")
        else:
            with open('/tmp/out', \'wb\') as handle:
                pickle.dump(output, handle)

    else:
        if({fitdata}):
            func=func.fit(X)
            with open('/tmp/func', \'wb\') as handle:
                pickle.dump(func, handle)
        
        X = func.transform(X)

        with open('/tmp/X', \'wb\') as handle:
            pickle.dump(X, handle)

if({measure_energy}):
    import pyemlWrapper

    energy = pyemlWrapper.measure_function(work)[1]
    with open('/tmp/energy', \'wb\') as handle:
        pickle.dump(energy, handle)

else:
    work()
"""    

            template = {

                        'container': 
                            {'args': [''],
                            'command': ['python', '-c'],
                            'image': 'alu0101040882/kubepipe:p3.9.12-tensorflow',
                            },
                        'inputs' : {
                            'artifacts':
                               [
                                    {"name" : f"inX{pipeId}", "path" :  "/tmp/X",    "s3":    { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/X{pipeId}" if i >= 1 else f"{self.minio_bucket_path}/{self.id}/tmp/X" }} ,
                                    {"name" : f"iny{pipeId}", "path" :  "/tmp/y",    "s3":    {  "key" : f"{self.minio_bucket_path}/{self.id}/tmp/y" }},
                                    {"name" : f"infunc{i}{pipeId}", "path" : "/tmp/func", "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/func{id(func)}{pipeId}" if fitdata or not (hasattr(func,"predict")) else f"{self.minio_bucket_path}/{self.id}/model{pipeId}" }}
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



            print("Node selector", node_selector)
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

                template["outputs"]["artifacts"].append({"name" : f"energy{i}{pipeId}", "path" : "/tmp/energy", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/energy{i}{pipeId}"}})


            if(fitdata):
                self.upload_variable(func,f"func{id(func)}{pipeId}")

            #Estimator
            if(hasattr(func,"predict")):
                template["outputs"]["artifacts"].append({"name" : f"output{pipeId}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/output{pipeId}"}})

                if(fitdata):
                    template["outputs"]["artifacts"].append({"name" : f"model{pipeId}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/model{pipeId}"}})
                
            #Transformer
            else:
                template["outputs"]["artifacts"].append({"name" : f"outX{pipeId}", "path" : "/tmp/X", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/tmp/X{pipeId}"}})

                if(fitdata):
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

   
        return self.launch_from_manifest(workflow)


    def delete_files(self, prefix):
        objects_to_delete = self.minioclient.list_objects(self.bucket, prefix=prefix, recursive=True)
        for obj in objects_to_delete:
            self.minioclient.remove_object(self.bucket, obj.object_name)
        print(f"Artifacts deleted from {prefix}")


    def run_workflows(self, X, y, operation, name,  fitdata, resources = None, pipeIndex = None, applyToFuncs = None, output = "output", outputPrefix = "tmp", concurrent_pipelines = None, return_output = True, measure_energy = False,additional_args = None, node_selector = None):

        if pipeIndex == None:
            pipeIndex = range(len(self.pipelines))

        
        if concurrent_pipelines == None:
            if(self.concurrent_pipelines != None):
                concurrent_pipelines = self.concurrent_pipelines
            else:
                concurrent_pipelines = len(self.pipelines)

        workflows = []


        self.upload_variable(X,f"X", prefix = "tmp")
        self.upload_variable(y,f"y", prefix = "tmp")


        for i , index in enumerate(pipeIndex):

            #Check that no more than "concurrent_pipelines" are running at the same time, wait for a workflow to finish
            if(len(workflows) >= concurrent_pipelines):
                finishedWorkflows = self.wait_for_workflows(workflows,numberToWait=1)
                for workflow in finishedWorkflows:
                    workflows.remove(workflow)
        
            pipeline = self.pipelines[index]

            funcs = pipeline["funcs"]

            if applyToFuncs is not None and callable(applyToFuncs):
                funcs = applyToFuncs(funcs)

            workflows.append(self.workflow(X,y, funcs, f"{i}-{str.lower(str(type( pipeline['funcs'][-1] ).__name__))}-{name}-", pipeline["id"], resources = resources, fitdata=fitdata, operation = operation, measure_energy = measure_energy, additional_args = additional_args, node_selector = node_selector))
        
        if(len(workflows) > 0):
            self.wait_for_workflows(workflows)

        if(measure_energy):
            energy = []
            for i, index in enumerate(pipeIndex):
                energy.append({})
                for j, func in enumerate(self.pipelines[index]):
                    func_energy = self.download_variable(f"energy{j}{self.pipelines[index]['id']}", prefix = "tmp")
                    print("Func energy", func_energy)
                    if(j == 0):
                        energy[-1] = func_energy
                    else:
                        for device, consumed in func_energy.items():
                            energy[-1][device]["consumed"] += consumed["consumed"]
                            energy[-1][device]["elapsed"] += consumed["elapsed"]

            self.energy =  energy

        if(return_output):
            outputs = []

            for i, index in enumerate(pipeIndex):
                outputs.append(self.download_variable(f"{output}{self.pipelines[index]['id']}", prefix = outputPrefix))

            return outputs

    def get_models(self):
        output = []

        for pipeline in self.pipelines:
            output.append(self.download_variable(f"model{pipeline['id']}" ))

        return output
  

    def get_consumed(self):
        if(self.energy is None):
            raise ValueError("Energy must be measured with 'measure_energy = True' before getting it")
            
        return self.energy

    def get_total_consumed(self):
        consumed = self.get_consumed()
        total_consumed = {}
        for pipe_consumed in consumed:
            for device, consumed in pipe_consumed.items():
                if(device not in total_consumed):
                    total_consumed[device] = {}
                    total_consumed[device]["consumed"] = consumed["consumed"]
                    total_consumed[device]["elapsed"] = consumed["elapsed"]
                else:
                    total_consumed[device]["consumed"] += consumed["consumed"]
                    total_consumed[device]["elapsed"] += consumed["elapsed"]
                    
        return total_consumed

    def fit(self,X,y, resources = None, concurrent_pipelines = None, measure_energy = False, node_selector = None, **kwargs):
        self.run_workflows(X,y,"fit(X,y,**add_args)", "fit", True, resources = resources, concurrent_pipelines = concurrent_pipelines, return_output = False, measure_energy = measure_energy, additional_args = kwargs, node_selector = node_selector)
        self.fitted = True
        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return self
        
    def score(self,X,y, resources = None, pipeIndex = None, concurrent_pipelines = None):

        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating score")

        out =  self.run_workflows(X,y,"score(X,y)", "score",  False,  resources = resources, pipeIndex=pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out

    def score_samples(self,X, resources = None, pipeIndex = None, concurrent_pipelines = None):

        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating score_samples")

        out =  self.run_workflows(X,None,"score_samples(X)", "score_samples",  False,  resources = resources, pipeIndex=pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out

    def transform(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):

        if self.pipelines == None or not self.fitted:
            raise Exception("Transformer must be fitted before transform")

        out =  self.run_workflows(X, None, "transform(X)", "transform" ,False, resources = resources, pipeIndex=pipeIndex, applyToFuncs= lambda f : f[:-1], output = "X",concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out

    
    def inverse_transform(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):

        if self.pipelines == None or not self.fitted:
            raise Exception("Transformer must be fitted before inverse_transform")

        out =  self.run_workflows(X, None, "transform(X)", "inverse_transform" ,False, resources = resources, pipeIndex=pipeIndex, applyToFuncs= lambda f : f[:-1][::-1], output = "X",concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out


    def predict_proba(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating predict_proba")

        out =  self.run_workflows(X, None, "predict_proba(X)", "predict_proba", False, resources = resources, pipeIndex = pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out

    def predict_log_proba(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating predict_log_proba")

        out =  self.run_workflows(X, None, "predict_log_proba(X)", "predict_log_proba", False, resources = resources, pipeIndex = pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out

    def predict(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating predict")

        out =  self.run_workflows(X, None, "predict(X)", "predict", False, resources = resources, pipeIndex = pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out

    def decision_function(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating predict")

        out =  self.run_workflows(X, None, "decision_function(X)", "decision_function", False, resources = resources, pipeIndex = pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out
        
    def fit_predict(self, X, y, resources = None, pipeIndex = None, concurrent_pipelines = None):

        out = self.run_workflows(X, y, "fit_predict(X,y)", "fit_predict", True, resources = resources, pipeIndex = pipeIndex,concurrent_pipelines = concurrent_pipelines)

        self.delete_files(f"{self.minio_bucket_path}/{self.id}/tmp")

        return out


    def config(self, resources = None, function_resources = None, concurrent_pipelines = None, namespace = None, tmpFolder = None,node_selector = None):
        if node_selector: self.node_selector = node_selector
        if namespace: self.namespace = namespace
        if tmpFolder: self.tmpFolder = tmpFolder
        if resources: self.kuberesources = resources 
        if function_resources: self.function_resources = function_resources
        if concurrent_pipelines: self.concurrent_pipelines = concurrent_pipelines


    def launch_from_manifest(self,manifest):
        api_response = self.api.create_workflow(
            namespace=self.namespace,
            body=IoArgoprojWorkflowV1alpha1WorkflowCreateRequest(workflow=manifest, _check_type=False
            ))

        name = api_response["metadata"]["name"]

        
        print(f"Launched workflow '{name}'")
        return name


    def wait_for_workflows(self,workflowNames, numberToWait = None):

        if(numberToWait == None):
            numberToWait = len(workflowNames)    
        
        finished = []

        while len(finished) < numberToWait:

            for workflowName in workflowNames:
                if(workflowName not in finished):
                    workflow = None

                    try:
                    
                        workflow = self.api.get_workflow(namespace=self.namespace,name = workflowName)

                    except NotFoundException:
                        finished.append(workflowName)
                        print(f"\nWorkflow '{workflowName}' has been deleted.")
                    
                    if(workflow is not None):
                        status = workflow["status"]
                    
                        if(getattr(status,"phase",None) is not None):

                            if(status["phase"] == "Succeeded"):
                                endtime = datetime.datetime.now(tzutc())
                                starttime = workflow["metadata"]["creation_timestamp"]

                                print(f"\nWorkflow '{workflowName}' has finished. Time ({endtime-starttime})"u'\u2713')
                                #self.deleteFiles(workflowName)
                                
                                finished.append(workflowName)

                            elif(status["phase"] == "Failed"):
                                self.delete_files(f"{self.minio_bucket_path}/{self.id}/")
                                raise Exception(f"Workflow {workflowName} has failed")

            if(len(finished) < numberToWait):
                sleep(1)
                print(".",end="",sep="",flush=True)

        return finished


    def get_workflow_status(self, workflowName):
        try:
            workflow = self.api.get_workflow(namespace=self.namespace,name = workflowName)
            return workflow["status"]

        except NotFoundException:
            return None


    def wait_for_workflow(self,workflowName):

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




        