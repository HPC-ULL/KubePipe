import dill as pickle

import uuid

from abc import ABC, abstractmethod
from platform import python_version

import atexit
from importlib_metadata import packages_distributions, version
from dill.detect import getmodule

import requests

import os
from time import sleep

from kubernetes import client, config, watch

from typing import Union,Dict


import logging
logger = logging.getLogger('kubepipe')


class Pipeline(ABC):

    def __init__(self,
                 *funcs,
                 image: str = "",
                 use_gpu: Union[bool,None] = None,
                 resources : Union[Dict,None] = None,
                 **kwargs):

        self.image = image
        self.funcs = funcs
        self.use_gpu = use_gpu
        self.resources = resources
        self.backend = ""
   

    def initialize(
        self,
        *args,                 
        namespace: str = "argo",
        kube_api: Union[client.CoreV1Api, None] = None,
        registry_ip: Union[str, None] = None
        ):

        self.namespace = namespace

        if not registry_ip:
            registry_ip = self.get_service_ip(
                "private-repository-k8s", self.namespace) + ":5000"

        self.registry_ip = registry_ip

        self.id = str(uuid.uuid4())[:8]


        if(kube_api == None):
            config.load_kube_config()
            kube_api = client.CoreV1Api()
        self.kube_api = kube_api

        if(self.image == ""):
            self.image = self.create_image(self.funcs)


        return self

    @abstractmethod
    def run(self,*args,**kwargs):
        pass

    @abstractmethod
    def clean(self,*args,**kwargs):
        pass

    @abstractmethod
    def is_running(self,*args,**kwargs):
        pass

    @abstractmethod
    def get_consumed(self,*args,**kwargs):
        pass

    @abstractmethod
    def get_output(self,*args,**kwargs):
        pass

    def get_service_ip(self,
                       service_name: str,
                       namespace: str):
        ingress = self.kube_api.read_namespaced_service(
            service_name, namespace).status.load_balancer.ingress[0]
        if ingress.ip is None:
            if ingress.hostname is not None:
                ip = ingress.hostname
            else:
                raise ValueError(
                    f"Ip from service {service_name} can't be retrieved")
        else:
            ip = ingress.ip
        return ip

    def image_exists(self,
                     registry: str,
                     image_name: str,
                     tag: str):
        response = requests.get(
            f'http://{registry}/v2/{image_name}/tags/list')
        response = response.json()

        if ("tags" in response):
            for t in response["tags"]:
                if t == tag:
                    return True

        return False

    def get_module_dependencies(self, funcs):
        deps = ["dill", "minio", "scikit-learn"]
        for func in funcs:
            module = getmodule(func)
            pkgs = packages_distributions().get(
                module.__name__.split('.')[0], None)
            if (pkgs is not None):
                for pkg in pkgs:
                    if (pkg not in deps):
                        deps.append(pkg)
                        if(pkg == "scikeras"):
                            deps.append("tensorflow")

        return sorted(deps)

    def create_image(self, funcs):
        deps = self.get_module_dependencies(funcs)
        image_name = f"kubepipe"
        image_tag = f"{python_version()}_{'_'.join(dep + version(dep) for dep in deps)}"

        base_image = ""

        if(self.use_gpu):
            if("tensorflow" in deps):
                base_image = f'tensorflow/tensorflow:{version("tensorflow")}-gpu'
                deps.remove("tensorflow")
                image_tag += '-gpu'
            elif("torch" in deps):
                base_image = f"""pytorch/pytorch:{version("torch")}-11.3-cudnn8-runtime\nRUN rm /etc/apt/sources.list.d/cuda.list\nRUN rm /etc/apt/sources.list.d/nvidia-ml.list"""
                deps.remove("torch")
                image_tag += '-gpu'
            else:
                base_image = f'python:{python_version()}-slim'

        else:
            base_image = f'python:{python_version()}-slim'

        full_name = f"{self.registry_ip}/{image_name}:{image_tag}"

        if (not self.image_exists(self.registry_ip, image_name, image_tag)):

            dockerfile = f"""
FROM {base_image}
RUN apt update && apt install -y  git libconfuse-dev && rm -rf /var/lib/apt/lists/*
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
RUN pip install {" ".join(dep + '=='  + version(dep) for dep in deps)}
RUN git clone https://github.com/HPC-ULL/Pyeml && pip install -e Pyeml && cp Pyeml/src/pyeml/lib/libeml.so.1.1 /usr/local/lib
RUN pip install pip install git+https://github.com/HPC-ULL/KubePipe --no-deps
    """

            print("Dockerfile:\n", dockerfile)
            config_map_name = "dockerfileconfigmap" + self.id
            self.kube_api.create_namespaced_config_map(
                body=client.V1ConfigMap(data={"Dockerfile": dockerfile}, metadata=client.V1ObjectMeta(
                    name=config_map_name, labels={"app": "kubepipe"})),
                namespace=self.namespace
            )

            registry_cluster_ip = self.kube_api.read_namespaced_service(
                name="private-repository-k8s", namespace=self.namespace).spec.cluster_ip + ":5000"

            command = ["buildctl-daemonless.sh"]
            args = ["build", "--frontend", "dockerfile.v0", "--local", "context=/tmp/work",
                    "--local", "dockerfile=/tmp/work", "--output", f"type=image,name={registry_cluster_ip}/{image_name}:{image_tag},push=true,registry.insecure=true"]
            container = client.V1Container(
                name=f"image-creator",
                image="moby/buildkit",
                command=command,
                args=args,
                volume_mounts=([client.V1VolumeMount(
                    name="dockerfile", mount_path="/tmp/work")]),
                security_context=client.V1SecurityContext(privileged=True)
            )

            spec = client.V1PodSpec(restart_policy="Never", containers=[container], volumes=[
                client.V1Volume(name="dockerfile", config_map={"name": config_map_name})],)

            pod_name = f"image-creator{self.id}"
            body = client.V1Job(
                api_version="v1",
                kind="Pod",
                metadata=client.V1ObjectMeta(
                    name=pod_name, labels={"app": "kubepipe"}),
                spec=spec
            )

            api_response = self.kube_api.create_namespaced_pod(
                body=body,
                namespace=self.namespace,
                async_req=False
            )

            print(f"Creating image '{full_name}'...")

            while True:
                resp = self.kube_api.read_namespaced_pod(
                    name=pod_name,
                    namespace=self.namespace
                )
                if resp.status.phase != 'Pending':
                    break

                if resp.status.phase == 'Failed':
                    print(f"Pod '{pod_name}' failed, aborting...")
                    return

                sleep(0.1)

            w = watch.Watch()
            for e in w.stream(self.kube_api.read_namespaced_pod_log, name=pod_name, namespace=self.namespace):
                print(e)

            w.stop()

            if(self.kube_api.read_namespaced_pod(
                name=pod_name,
                namespace=self.namespace
            ).status.phase == "Failed"):
                raise ValueError("Image creation failed")
            else:
                print("Image created successfully")

            self.kube_api.delete_namespaced_pod(
                pod_name, namespace=self.namespace)
            self.kube_api.delete_namespaced_config_map(
                config_map_name, namespace=self.namespace)

        return full_name
