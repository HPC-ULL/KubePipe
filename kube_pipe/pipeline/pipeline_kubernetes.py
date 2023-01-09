

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


class PipelineKubernetes(Pipeline):

    def __init__(self,
            *funcs,
            image: str = "",
            use_gpu: bool = False,
            ):
        super().__init__(*funcs,image=image,use_gpu=use_gpu)
        self.backend = "kubernetes"

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
        use_gpu : Union[bool,None] = False,
        **kwargs
    ):

        super().initialize(*args,namespace=namespace, kube_api=kube_api, registry_ip=registry_ip)

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
        return self.download_variable(f'energy', prefix =self.id)

    def get_output(self):
        return self.download_variable('output', prefix = self.id)

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

        if(self.resources != None):
            resources = self.resources


        self.upload_variable(X,"X", prefix = "tmp")
        self.upload_variable(y,"y", prefix = "tmp")

        self.upload_variable(self.funcs, "funcs", prefix = f"tmp/{self.id}", overwrite = True)

        if(additional_args):
            self.upload_variable(additional_args, f"add_args", prefix = f"tmp/{self.id}")

        fit_data = operation.startswith("fit")

        run_name = str.lower(str(type(self.funcs[-1] ).__name__)) + "-" +  operation[:operation.index("(")]

        code = f"""
import dill as pickle

from sklearn.pipeline import make_pipeline

from minio import Minio

import numpy as np

import sys
import os
from time import sleep


minioclient = Minio(
            'minio:9000',
            access_key='{self.access_key}',
            secret_key='{self.secret_key}',
            secure=False
)

def download_object(name, prefix = "", retrys = 10):
    print(f"Downloading '{self.minio_bucket_path}/{'{prefix}'}{'{name}'}' ")
    retry = 0
    while True:
        try:
            minioclient.fget_object(f'{self.bucket}', f'{self.minio_bucket_path}/{'{prefix}'}{'{name}'}', f'/tmp/{'{name}'}')
            break
        except Exception as e:
            retry+=1
            if(retry == retrys):
                raise ValueError(f"Failed to download {'{name}'}")
                break

            print(f"failed to download '{self.minio_bucket_path}/{'{prefix}'}{'{name}'}'", e)
            sleep(0.5)

    with open(f'/tmp/{'{name}'}', 'rb') as input_file:
        print(f"Trying to load  {'{name}'}")
        var = pickle.load(input_file)
        print(f"Loaded {'{name}'}")
    os.remove(f'/tmp/{'{name}'}')

    return var

def work():
    X = download_object("X", prefix="tmp/")
    y = download_object("y", prefix="tmp/")

    if({len(additional_args) != 0 }):
        add_args = download_object("add_args", prefix="tmp/{self.id}/")

    else:
        add_args = dict()

    if({fit_data}):
        funcs = download_object("funcs", prefix = "tmp/{self.id}/")
        pipe = make_pipeline(*funcs)
    else:
        pipe = download_object("pipe", prefix = "{self.id}/")

    output = pipe.{operation}

    try:
        from scikeras.wrappers import BaseWrapper

        if({fit_data} and isinstance(output[-1],BaseWrapper)):
            output = output[-1].model_
            output.save('/tmp/out',save_format="h5")
        else:
            with open('/tmp/out', \'wb\') as handle:
                pickle.dump(output, handle)
    except ModuleNotFoundError:
        with open('/tmp/out', \'wb\') as handle:
            pickle.dump(output, handle)


    minioclient.fput_object(
                '{self.bucket}', '{self.minio_bucket_path}/{self.id}/output', '/tmp/out',
    )

    if({fit_data}): 
        minioclient.fput_object(
                '{self.bucket}', '{self.minio_bucket_path}/{self.id}/pipe', '/tmp/out',
        )

if({measure_energy}):
    from pyeml import measure_function
    energy = measure_function(work)[1]
    with open('/tmp/energy', \'wb\') as handle:
        pickle.dump(energy, handle)

    minioclient.fput_object(
        '{self.bucket}', '{self.minio_bucket_path}/{self.id}/energy', '/tmp/energy',
    )

else:
    work()
"""
        volumes = []
        volume_mounts = []

        if (measure_energy):
            volumes.append(client.V1Volume(
                name="vol", host_path=client.V1HostPathVolumeSource(path="/dev/cpu")))
            volume_mounts.append(client.V1VolumeMount(
                name="vol", mount_path="/dev/cpu"))

        command = ["python3", "-c", code]
        container = client.V1Container(
            name=f"{self.id}",
            image=self.image,
            command=command,
            resources=client.V1ResourceRequirements(limits=resources),
            volume_mounts=volume_mounts,
            security_context=client.V1SecurityContext(
                privileged=measure_energy)
        )

        spec = client.V1PodSpec(restart_policy="Never", containers=[
                                container], node_selector=node_selector, volumes=volumes)

        workflowname = f"pipeline-{run_name}-{self.id}-{str(uuid.uuid4())[:3]}"

        body = client.V1Job(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(
                name=workflowname, labels={"app": "kubepipe"}),
            spec=spec
        )

        api_response = self.kube_api.create_namespaced_pod(
            body=body,
            namespace=self.namespace)

        self.workflow_name = workflowname

        return self

    def is_running(self):
        if(not self.workflow_name):
            print("Pipeline has not run yet")
            return False

        try:
            workflow = self.kube_api.read_namespaced_pod_status(
                name=self.workflow_name,
                namespace=self.namespace)

            status = workflow.status.phase

            if (status == "Succeeded"):
                self.kube_api.delete_namespaced_pod(self.workflow_name, self.namespace)
                return False

            elif (status == "Failed" or status == "Error"):
                raise Exception(f"Workflow '{self.workflow_name}' has failed")

        except Exception as e:
            if (e.__class__.__name__ == "ApiException" and e.status == 404):
                None
            else:
                raise e

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
