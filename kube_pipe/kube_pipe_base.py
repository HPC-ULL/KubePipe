from time import sleep
import dill as pickle

import uuid

from abc import ABC, abstractmethod
from platform import python_version

import atexit
from kubernetes import client, config, watch
from importlib_metadata import packages_distributions, version
from dill.detect import getmodule

import requests

class KubePipeBase(ABC):

    def __init__(self,*args, images = None, registry_ip = None, use_gpu = False):

        self.id = str(uuid.uuid4())[:8]

        self.tmpFolder = "tmp"

        self.kuberesources = None

        self.concurrent_pipelines = None

        self.function_resources = {}

        self.namespace = "argo"

        self.models = None

        self.use_gpu = use_gpu

        config.load_kube_config()
        self.kubeapi = client.CoreV1Api()

        if not registry_ip:
            registry_ip = self.get_service_ip(
                "private-repository-k8s", self.namespace) + ":5000"
        
        self.registry_ip = registry_ip

        self.pipelines = []
        for arg in args:
            self.pipelines.append({
                "id" : str(uuid.uuid4())[:8],
                "funcs" : arg,
                "image" : None
            })

        if (not images):
            for pipeline in self.pipelines:
                pipeline["image"] = self.create_image(pipeline)
        else:
            if (len(images) != len(self.pipelines)):
                raise ValueError(
                    "Len of images must be the same as the number of pipelines")

            for image, pipeline in zip(images, self.pipelines):
                if (image is None):
                    image = self.create_image(pipeline)

                pipeline["image"] = image


        atexit.register(self.clean_workflows)



    @abstractmethod
    def clean_workflows(self):
        pass
    

    @abstractmethod
    def fit(self,X,y, resources = None, concurrent_pipelines = None):
        pass
    
    @abstractmethod
    def score(self,X,y, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod
    def score_samples(self,X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod
    def transform(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod
    def inverse_transform(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod
    def predict_proba(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod
    def predict_log_proba(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass
    @abstractmethod
    def predict(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod
    def decision_function(self, X, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod        
    def fit_predict(self, X, y, resources = None, pipeIndex = None, concurrent_pipelines = None):
        pass

    @abstractmethod
    def config(self, resources = None, function_resources = None, concurrent_pipelines = None, namespace = None, tmpFolder = None):
        pass

    
    def get_service_ip(self, service_name, namespace):
        ingress = self.kubeapi.read_namespaced_service(
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



    def image_exists(self, registry, image_name, tag):
        response = requests.get(
            f'http://{registry}/v2/{image_name}/tags/list')
        response = response.json()

        if ("tags" in response):
            for t in response["tags"]:
                if t == tag:
                    return True

        return False

    def get_module_dependencies(self, pipeline):
        deps = ["dill", "minio", "scikit-learn"]
        for func in pipeline["funcs"]:
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

    def create_image(self, pipeline):
        deps = self.get_module_dependencies(pipeline)
        image_name = f"kubepipe"
        image_tag = f"{python_version()}_{'_'.join(dep + version(dep) for dep in deps)}"

        full_name = f"{self.registry_ip}/{image_name}:{image_tag}"


        if (not self.image_exists(self.registry_ip, image_name, image_tag)):

            base_image = ""
            
            if(self.use_gpu):
                if("tensorflow" in deps):
                    base_image=f'tensorflow:{version("tensorflow")}-gpu'
                    deps.remove("tensorflow")
                elif("pytorch" in deps):
                    base_image=f'pytorch:{version("pytorch")}-cuda10.2-cudnn7-devel'
                    deps.remove("pytorch")
                else:
                    base_image=f'python:{python_version()}-slim'

            else:
                base_image=f'python:{python_version()}-slim'

            dockerfile = f"""
FROM {base_image}
RUN apt update && apt install -y  git libconfuse-dev 
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
RUN pip install {" ".join(dep + '=='  + version(dep) for dep in deps)}
RUN git clone https://github.com/HPC-ULL/Pyeml && pip install -e Pyeml && cp Pyeml/src/pyeml/lib/libeml.so.1.1 /usr/local/lib
RUN pip install pip install git+https://github.com/HPC-ULL/KubePipe --no-deps
"""


            config_map_name = "dockerfileconfigmap" + pipeline["id"]
            self.kubeapi.create_namespaced_config_map(
                body=client.V1ConfigMap(data={"Dockerfile": dockerfile}, metadata=client.V1ObjectMeta(
                    name=config_map_name, labels={"app": "kubepipe"})),
                namespace=self.namespace
            )

            registry_cluster_ip = self.kubeapi.read_namespaced_service(name="private-repository-k8s", namespace=self.namespace).spec.cluster_ip + ":5000"
           
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

            spec = client.V1PodSpec( restart_policy="Never", containers=[container], volumes=[
                                    client.V1Volume(name="dockerfile", config_map={"name": config_map_name})],)

            pod_name = f"image-creator{pipeline['id']}"
            body = client.V1Job(
                api_version="v1",
                kind="Pod",
                metadata=client.V1ObjectMeta(
                    name=pod_name, labels={"app": "kubepipe"}),
                spec=spec
            )

            api_response = self.kubeapi.create_namespaced_pod(
                body=body,
                namespace=self.namespace,
                async_req=False
            )

            print(f"Creating image '{full_name}'...")

            while True:
                resp = self.kubeapi.read_namespaced_pod(
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
            for e in w.stream(self.kubeapi.read_namespaced_pod_log, name=pod_name, namespace=self.namespace):
                print(e)

            w.stop()

            if(self.kubeapi.read_namespaced_pod(
                    name=pod_name,
                    namespace=self.namespace
                ).status.phase == "Failed"):
                raise ValueError("Image creation failed")
            else:
                print("Image created successfully")


            self.kubeapi.delete_namespaced_pod(
                pod_name, namespace=self.namespace)
            self.kubeapi.delete_namespaced_config_map(
                config_map_name, namespace=self.namespace)

        return full_name

    def get_consumed(self):
        if (getattr(self, 'energy',  None) is None):
            raise ValueError(
                "Energy must be measured with 'measure_energy = True' before getting it")

        return self.energy

    def get_total_consumed(self):
        consumed = self.get_consumed()
        total_consumed = {}
        for pipe_consumed in consumed:
            for device, consumed in pipe_consumed.items():
                if(device not in total_consumed):
                    total_consumed[device] = consumed
                else:
                    total_consumed[device] += consumed

        return total_consumed




        