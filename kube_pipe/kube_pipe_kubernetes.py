from .kube_pipe_base import KubePipeBase

import os
from time import sleep
import yaml
import dill as pickle

import uuid



from pathlib import Path
from minio import Minio

from kubernetes import client, config
import base64

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KubePipeKubernetes(KubePipeBase):

    def __init__(self, *args,  minio_ip=None, registry_ip=None, access_key=None, secret_key=None, minio_bucket_path=".kubetmp", images = None, tmpFolder="/tmp", namespace="argo", use_gpu = False,  **kwargs):

        super().__init__(*args, images = images, registry_ip = registry_ip, use_gpu = use_gpu)
        self.tmpFolder = tmpFolder
        self.namespace = namespace

        self.backend = "kubernetes"

        Path(tmpFolder).mkdir(parents=True, exist_ok=True)

        self.minio_bucket_path = minio_bucket_path

        if not minio_ip:
            minio_ip = self.get_service_ip("minio", self.namespace) + ":9000"


        artifactsConfig = yaml.safe_load(self.kubeapi.read_namespaced_config_map(
            "artifact-repositories", self.namespace).data["default-v1"])["s3"]

        if not access_key:
            access_key = base64.b64decode(self.kubeapi.read_namespaced_secret(
                artifactsConfig["accessKeySecret"]["name"], self.namespace).data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        if not secret_key:
            secret_key = base64.b64decode(self.kubeapi.read_namespaced_secret(
                artifactsConfig["secretKeySecret"]["name"], self.namespace).data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

        self.access_key = access_key
        self.secret_key = secret_key
        self.minioclient = Minio(
            minio_ip,
            access_key=access_key,
            secret_key=secret_key,
            secure=not artifactsConfig["insecure"]
        )

        self.bucket = artifactsConfig["bucket"]

        self.node_selector = None


        if not self.minioclient.bucket_exists(self.bucket):
            self.minioclient.make_bucket(self.bucket)


    def clean_workflows(self):
        self.delete_files(f"{self.minio_bucket_path}/{self.id}/")


    def get_energy(self,pipeIndex):
        energy = []
        for index in pipeIndex:
            energy.append(self.download_variable(
                f"{self.pipelines[index]['id']}-energy", prefix="tmp"))
        return energy

    def workflow(self, funcs, name, pipeId, operation="fit(X,y)", fit_data=False, resources=None, node_selector=None, measure_energy=False, additional_args=None, image="python"):

        if (resources == None):
            resources = self.kuberesources

        if (node_selector == None):
            node_selector = self.node_selector

        self.upload_variable(funcs, f"funcs{pipeId}", "tmp")

        if (additional_args):
            self.upload_variable(additional_args, f"add_args{pipeId}", "tmp")
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
    print(f"Downloading '{self.minio_bucket_path}/{self.id}{'{prefix}'}{'{name}'}' ")
    retry = 0
    while True:
        try:
            minioclient.fget_object(f'{self.bucket}', f'{self.minio_bucket_path}/{self.id}{'{prefix}'}{'{name}'}', f'/tmp/{'{name}'}')
            break
        except Exception as e:
            retry+=1
            if(retry == retrys):
                raise ValueError(f"Failed to download {name}")
                break

            print(f"failed to download '{self.minio_bucket_path}/{self.id}{'{prefix}'}{'{name}'}'", e)
            sleep(0.5)

    with open(f'/tmp/{'{name}'}', 'rb') as input_file:
        print(f"Trying to load  {'{name}'}")
        var = pickle.load(input_file)
        print(f"Loaded {'{name}'}")
    os.remove(f'/tmp/{'{name}'}')

    return var

def work():
    X = download_object("X", prefix="/tmp/")
    y = download_object("y", prefix="/tmp/")

    if({len(additional_args) != 0 }):
        add_args = download_object("add_args{pipeId}", prefix="/tmp/")

    else:
        add_args = dict()

    if({fit_data}):
        funcs = download_object("funcs{pipeId}", prefix = "/tmp/")
        pipe = make_pipeline(*funcs)
    else:
        pipe = download_object("{pipeId}pipe")

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
                '{self.bucket}', '{self.minio_bucket_path}/{self.id}/tmp/{pipeId}', '/tmp/out',
    )

    if({fit_data}):
        minioclient.fput_object(
                '{self.bucket}', '{self.minio_bucket_path}/{self.id}/{pipeId}pipe', '/tmp/out',
        )

if({measure_energy}):
    from pyeml import measure_function
    energy = measure_function(work)[1]
    with open('/tmp/energy', \'wb\') as handle:
        pickle.dump(energy, handle)

    minioclient.fput_object(
        '{self.bucket}', '{self.minio_bucket_path}/{self.id}/tmp/{pipeId}-energy', '/tmp/energy',
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
            name=f"{pipeId}",
            image=image,
            command=command,
            resources=client.V1ResourceRequirements(limits=resources),
            volume_mounts=volume_mounts,
            security_context=client.V1SecurityContext(
                privileged=measure_energy)
        )

        spec = client.V1PodSpec(restart_policy="Never", containers=[
                                container], node_selector=node_selector, volumes=volumes)

        workflowname = f"pipeline-{name}-{self.id}-{pipeId}-{str(uuid.uuid4())[:3]}"

        body = client.V1Job(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(
                name=workflowname, labels={"app": "kubepipe"}),
            spec=spec
        )

        api_response = self.kubeapi.create_namespaced_pod(
            body=body,
            namespace=self.namespace)
        return workflowname


    def delete_files(self, prefix):
        objects_to_delete = self.minioclient.list_objects(
            self.bucket, prefix=prefix, recursive=True)
        for obj in objects_to_delete:
            self.minioclient.remove_object(self.bucket, obj.object_name)
        print(f"Artifacts deleted from {prefix}")

    def config(self, resources=None, function_resources=None, concurrent_pipelines=None, namespace=None, tmpFolder=None, node_selector=None):
        if node_selector:
            self.node_selector = node_selector

        if namespace:
            self.namespace = namespace
        if tmpFolder:
            self.tmpFolder = tmpFolder
        if resources:
            self.kuberesources = resources
        if function_resources:
            self.function_resources = function_resources
        if concurrent_pipelines:
            self.concurrent_pipelines = concurrent_pipelines

    def is_workflow_finished(self, workflow_name):
        try:
            workflow = self.kubeapi.read_namespaced_pod_status(
                name=workflow_name,
                namespace=self.namespace)

            status = workflow.status.phase

            if (status == "Succeeded"):
                self.kubeapi.delete_namespaced_pod(workflow_name, self.namespace)
                return True

            elif (status == "Failed" or status == "Error"):
                raise Exception(f"Workflow '{workflow_name}' has failed")

        except Exception as e:
            if (e.__class__.__name__ == "ApiException" and e.status == 404):
                None
            else:
                raise e

        return False


