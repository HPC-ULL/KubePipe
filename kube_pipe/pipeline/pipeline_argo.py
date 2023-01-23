

from .pipeline import Pipeline

from kubernetes import client, config, watch

import dill as pickle

import os
from time import sleep
from minio import Minio
from minio.error import S3Error
import base64
import yaml
import io

import uuid

from typing import Union, Dict

import argo_workflows
from argo_workflows.api import workflow_service_api
from argo_workflows.model.io_argoproj_workflow_v1alpha1_workflow_create_request import \
    IoArgoprojWorkflowV1alpha1WorkflowCreateRequest
from argo_workflows.exceptions import NotFoundException

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class PipelineArgo(Pipeline):


    def initialize(
        self,
        *args,
        namespace: str = "argo",
        kube_api: Union[client.CoreV1Api, None] = None,
        registry_ip: Union[str, None] = None,
        minio_ip:  Union[str, None] = None,
        access_key:  Union[str, None] = None,
        secret_key:  Union[str, None] = None,
        minio_bucket_path: str = ".kubetmp",
        argo_ip : Union[str,None] = None,
        use_gpu : Union[bool,None] = False,
        function_resources :  Union[Dict,None] = None,
        **kwargs

    ):

        super().initialize(*args,namespace=namespace, kube_api=kube_api, registry_ip=registry_ip)

        self.backend = "argo"


        self.function_resources = function_resources

        if not argo_ip: 
            argo_ip = "https://" + self.kube_api.read_namespaced_service("argo-server","argo").status.load_balancer.ingress[0].ip + ":2746"

        configuration = argo_workflows.Configuration(host=argo_ip, discard_unknown_keys=True)
        configuration.verify_ssl = False

        api_client = argo_workflows.ApiClient(configuration)
        self.argo_api = workflow_service_api.WorkflowServiceApi(api_client)

        if not minio_ip:
            minio_ip = self.get_service_ip("minio", self.namespace) + ":9000"

        artifactsConfig = yaml.safe_load(self.kube_api.read_namespaced_config_map(
            "artifact-repositories", self.namespace).data["default-v1"])["s3"]

        if not access_key:
            access_key = base64.b64decode(self.kube_api.read_namespaced_secret(
                artifactsConfig["accessKeySecret"]["name"], self.namespace).data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        if not secret_key:
            secret_key = base64.b64decode(self.kube_api.read_namespaced_secret(
                artifactsConfig["secretKeySecret"]["name"], self.namespace).data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

        self.minio_bucket_path = minio_bucket_path

        self.access_key = access_key
        self.secret_key = secret_key

        self.minioclient = Minio(
            minio_ip,
            access_key=access_key,
            secret_key=secret_key,
            secure=not artifactsConfig["insecure"]
        )

        self.bucket = artifactsConfig["bucket"]

        if not self.minioclient.bucket_exists(self.bucket):
            self.minioclient.make_bucket(self.bucket)

        if(self.use_gpu is None):
            self.use_gpu = use_gpu

        return self

    def delete_files(self, prefix):
        objects_to_delete = self.minioclient.list_objects(
            self.bucket, prefix=prefix, recursive=True)
        for obj in objects_to_delete:
            self.minioclient.remove_object(self.bucket, obj.object_name)

    def clean(self):
        self.delete_files(f"{self.minio_bucket_path}")

    def clean_tmp(self):
        self.delete_files(f"{self.minio_bucket_path}/tmp/{self.id}")

    def get_consumed(self):
        energy = {}

        for j, func in enumerate(self.funcs):
            func_energy = self.download_variable(f"energy{j}", prefix = self.id)
            if(j == 0):
                energy = func_energy
            else:
                for device, consumed in func_energy.items():
                    energy[device] += consumed
        return energy


        

    def get_output(self):
        output = self.download_variable('output', prefix = self.id)
        if(isinstance(output,bytes)):
            with open("/tmp/out.h5", "wb") as f:
                f.write(output)
            from tensorflow.keras.models import load_model
            output = load_model("/tmp/out.h5",compile = False)
        return output


    def run(
        self,
        X: any,
        y: any,
        operation: str = "fit(X,y,**add_args)",
        measure_energy: bool = False,
        resources: Union[Dict, None] = None,
        node_selector: Union[Dict, None] = None,
        additional_args: Dict = {}

    ):
        fit_data = operation.startswith("fit")

        if(self.resources != None):
            resources = self.resources

        if(self.node_selector != None):
            node_selector = self.node_selector


        self.upload_variable(X,"X", prefix = "tmp")
        self.upload_variable(y,"y", prefix = "tmp")



        if(additional_args):
            self.upload_variable(additional_args, f"add_args", prefix = f"tmp/{self.id}")


        run_name = str.lower(str(type(self.funcs[-1] ).__name__)) + "-" +  operation[:operation.index("(")]


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
        workflow["metadata"]["generateName"] = f"pipeline-{run_name}-{self.id}-{str(uuid.uuid4())[:3]}"

        templates[0]["steps"] = []

        for i , func in enumerate(self.funcs):
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

            if({fit_data} and isinstance(output,BaseWrapper)):
                output.model_.save("/tmp/out",save_format="h5")
                with open ("/tmp/out", "rb") as f:
                    output = f.read()

        except ModuleNotFoundError:
            pass

        with open('/tmp/out', \'wb\') as handle:
            pickle.dump(output, handle)

    else:
        if({fit_data}):
            func=func.fit(X)

        X = func.transform(X)

        with open('/tmp/X', \'wb\') as handle:
            pickle.dump(X, handle)

    if({fit_data} and not isinstance(func,BaseWrapper)):
        with open('/tmp/func', \'wb\') as handle:
            pickle.dump(func, handle)

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
                            'image': self.image,
                            },
                        'inputs' : {
                            'artifacts':
                               [
                                    {"name" : f"inX{self.id}", "path" :  "/tmp/X",    "s3":    { "key" : f"{self.minio_bucket_path}/tmp/{self.id}/X" if i >= 1 else f"{self.minio_bucket_path}/tmp/X" }} ,
                                    {"name" : f"iny{self.id}", "path" :  "/tmp/y",    "s3":    {  "key" : f"{self.minio_bucket_path}/tmp/y" }},
                                    {"name" : f"infunc{i}{self.id}", "path" : "/tmp/func", "s3": { "key" : f"{self.minio_bucket_path}/tmp/{self.id}/func{id(func)}" if fit_data else f"{self.minio_bucket_path}{self.id}/func_fitted{id(func)}" }}
                               ]
                        },
                        'outputs' : {
                            'artifacts':
                               []
                        },
                        'name': str(i) + str.lower(str(type(func).__name__))}

            if(func_add_args):
                template["inputs"]["artifacts"].append({"name" : f"add_args{i}{self.id}", "path" : "/tmp/add_args", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/tmp/{self.id}/add_args{id(func)}"}})
                self.upload_variable(func_add_args,f"add_args{id(func)}", prefix = f"tmp/{self.id}")

            if(fit_data ):
                self.upload_variable(func,f"func{id(func)}", prefix = f"tmp/{self.id}")
                template["outputs"]["artifacts"].append({"name" : f"outfunc{i}{self.id}", "path" : "/tmp/func", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}{self.id}/func_fitted{id(func)}"}})

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

                template["outputs"]["artifacts"].append({"name" : f"energy{i}{self.id}", "path" : "/tmp/energy", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/energy{i}"}})

            #Estimator
            if(hasattr(func,"predict")):
                template["outputs"]["artifacts"].append({"name" : f"output{self.id}", "path" : "/tmp/out", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/{self.id}/output"}})

            #Transformer
            else:
                template["outputs"]["artifacts"].append({"name" : f"outX{self.id}", "path" : "/tmp/X", "archive" : {"none" : {}}, "s3": { "key" : f"{self.minio_bucket_path}/tmp/{self.id}/X"}})
            
            
            if(self.resources != None):
                resources = self.resources
                
            if(self.function_resources is not None):
                for clase, config in self.function_resources.items():
                    if(isinstance(func,clase)):
                        resources = config


            if(resources is not None):
                template["container"]["resources"]  = resources

            template["container"]["args"][0] = code

            templates.append(template)
  
            step = [{'name': template["name"],
                    'template': template["name"]}]

            templates[0]["steps"].append(step)

            api_response = self.argo_api.create_workflow(
                namespace=self.namespace,
                body=IoArgoprojWorkflowV1alpha1WorkflowCreateRequest(workflow=workflow, _check_type=False
            ))

            name = api_response["metadata"]["name"]

            self.workflow_name = name

        return self

    def is_running(self):
        if(not self.workflow_name):
            print("Pipeline has not run yet")
            return False

        try:
            workflow = self.argo_api.get_workflow(namespace=self.namespace,name = self.workflow_name)

        except NotFoundException:
                print(f"\nWorkflow '{self.workflow_name}' has been deleted.")
                return False
            
        if(workflow is not None):
            status = workflow["status"]
        
            if(getattr(status,"phase",None) is not None):

                if(status["phase"] == "Succeeded"):
                    self.argo_api.delete_workflow(namespace=self.namespace, name = self.workflow_name)
                    return False

                elif(status["phase"] == "Failed"):
                    self.delete_files(f"{self.minio_bucket_path}/{self.id}/")
                    raise Exception(f"Workflow {self.workflow_name} has failed")
        return True


    def object_exists(self,
                      path: str):
        try:
            result = self.minioclient.stat_object(self.bucket, path)
        except S3Error as e:
            if(e.code == "NoSuchKey"):
                return False
        return True

    def upload_variable(self,
                        var: any,
                        name: str,
                        prefix: str = "",
                        overwrite: bool = False):

        if(prefix != ""):
            prefix += "/"

        path = f"{self.minio_bucket_path}/{prefix}{name}"

        if(overwrite or not self.object_exists(path)):
            up = self.minioclient.put_object(
                self.bucket, path, io.BytesIO(pickle.dumps(var, byref=False)),length=-1, part_size=10*1024*1024,
            )

    def download_variable(self,
                          name: str,
                          prefix: str = "",
                          delete: bool = False):

        if(prefix != "" and prefix[-1] != "/"):
            prefix += "/"

        try:
            response = self.minioclient.get_object(
                self.bucket, f"{self.minio_bucket_path}/{prefix}{name}")
            data = response.data
        finally:
            response.close()
            response.release_conn()

        if(delete):
            self.minioclient.remove_object(
                self.bucket, self.bucket, f"{self.minio_bucket_path}/{prefix}{name}")

        return pickle.loads(data)
    

