from time import sleep

import uuid

from .pipeline import Pipeline, PipelineKubernetes

import atexit
from kubernetes import client, config


class KubePipe():

    def __init__(self,
                 *args,
                 minio_ip=None,
                 registry_ip=None,
                 access_key=None,
                 secret_key=None,
                 minio_bucket_path=".kubetmp",
                 namespace="argo",
                 use_gpu=False,
                 argo_ip = None,
                 default_pipeline : Pipeline = PipelineKubernetes
                 ):

        self.pooling_interval = 0.1

        self.id = str(uuid.uuid4())[:8]

        self.resources = None

        self.concurrent_pipelines = None

        self.function_resources = {}

        self.namespace = namespace

        self.models = None

        self.use_gpu = use_gpu

        self.minio_bucket_path = minio_bucket_path

        config.load_kube_config()
        self.kube_api = client.CoreV1Api()

        if not registry_ip:
            registry_ip = self.get_service_ip(
                "private-repository-k8s", self.namespace) + ":5000"

        self.registry_ip = registry_ip

        self.pipelines = []
        for pipe in args:
            if(isinstance(pipe, Pipeline)):
                pass
            elif(isinstance(pipe, list)):
                pipe = default_pipeline(*pipe)
            else:
                raise ValueError("Unknown pipeline ", pipe)
           
            self.pipelines.append(pipe.initialize(namespace = self.namespace, kube_api = self.kube_api, registry_ip = self.registry_ip, minio_ip = minio_ip, access_key = access_key, secret_key = secret_key, argo_ip = argo_ip, minio_bucket_path = self.minio_bucket_path + "/" + self.id, use_gpu = self.use_gpu))

        atexit.register(self.clean_pipelines)

   
    def get_consumed(self):
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

    def run_pipelines(self, X, y, operation, resources=None, pipeIndex=None, applyToFuncs=None, concurrent_pipelines=None,  measure_energy=False, additional_args={}, node_selector=None, return_output = True):

        if pipeIndex is None:
            pipeIndex = range(len(self.pipelines))

        if(resources is None):
            resources = self.resources
        
        if(node_selector is None):
            node_selector = self.node_selector
        
        if concurrent_pipelines is None:
            if(self.concurrent_pipelines != None):
                concurrent_pipelines = self.concurrent_pipelines
            else:
                concurrent_pipelines = len(self.pipelines)

        running = set()

        for i, index in enumerate(pipeIndex):

            #Check that no more than "concurrent_pipelines" are running at the same time, wait for a workflow to finish
            if(len(running) >= concurrent_pipelines):
                finished_pipelines = self.wait_for_pipelines(running, numberToWait=1)
                running = running - finished_pipelines

            pipeline = self.pipelines[index]

            running.append(pipeline.run(X,y,operation=operation,measure_energy=measure_energy,resources=resources,node_selector=node_selector,additional_args=additional_args))
            print(f"Launched pipeline '{pipeline.workflow_name}' ({pipeline.backend})")


        if(len(running) > 0):
            self.wait_for_pipelines(running)
            


        if(measure_energy):
            energy = []
            for index in pipeIndex:
                energy.append(self.pipelines[index].get_consumed())
            self.energy = energy

        if(return_output):
            outputs = []

            for i, index in enumerate(pipeIndex):
                outputs.append(self.pipelines[index].get_output())

            return outputs


    def clean_pipelines(self):
        for pipeline in self.pipelines:
            pipeline.clean()

    def clean_pipelines_tmp(self):
        for pipeline in self.pipelines:
            pipeline.clean_tmp()
            
    def get_models(self):
        return self.models

    def fit(self, X, y, resources=None, concurrent_pipelines=None, measure_energy=False, node_selector=None, **kwargs):
        self.run_pipelines(X, y, "fit(X,y,**add_args)", resources=resources, concurrent_pipelines=concurrent_pipelines,
                           return_output=False, measure_energy=measure_energy, additional_args=kwargs, node_selector=node_selector)
        self.fitted = True
        self.clean_pipelines_tmp()

        return self

    def score(self, X, y, resources=None, pipeIndex=None, concurrent_pipelines=None):

        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating score")

        out = self.run_pipelines(X, y, "score(X,y)",  resources=resources,
                                 pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)

        self.clean_pipelines_tmp()


        return out

    def score_samples(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        if self.pipelines == None or not self.fitted:
            raise Exception(
                "Model must be trained before calculating score_samples")

        out = self.run_pipelines(X, None, "score_samples(X)", 
                                 resources=resources, pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)

        self.clean_pipelines_tmp()

        return out

    def transform(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        if self.pipelines == None or not self.fitted:
            raise Exception("Transformer must be fitted before transform")

        out = self.run_pipelines(X, None, "transform(X)",  resources=resources, pipeIndex=pipeIndex,
                                 applyToFuncs=lambda f: f[:-1], output="X", concurrent_pipelines=concurrent_pipelines)

        self.clean_pipelines_tmp()

        return out

    def inverse_transform(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        if self.pipelines == None or not self.fitted:
            raise Exception(
                "Transformer must be fitted before inverse_transform")

        out = self.run_pipelines(X, None, "transform(X)",  resources=resources, pipeIndex=pipeIndex,
                                 applyToFuncs=lambda f: f[:-1][::-1], output="X", concurrent_pipelines=concurrent_pipelines)

        self.clean_pipelines_tmp()

        return out

    def predict_proba(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):
        if self.pipelines == None or not self.fitted:
            raise Exception(
                "Model must be trained before calculating predict_proba")

        out = self.run_pipelines(X, None, "predict_proba(X)", 
                                 resources=resources, pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)

        self.clean_pipelines_tmp()

        return out

    def predict_log_proba(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):
        if self.pipelines == None or not self.fitted:
            raise Exception(
                "Model must be trained before calculating predict_log_proba")

        out = self.run_pipelines(X, None, "predict_log_proba(X)", 
                                 resources=resources, pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)
        self.clean_pipelines_tmp()

        return out

    def predict(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):
        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating predict")

        out = self.run_pipelines(X, None, "predict(X)",  resources=resources,
                                 pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)
        self.clean_pipelines_tmp()

        return out

    def decision_function(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):
        if self.pipelines == None or not self.fitted:
            raise Exception("Model must be trained before calculating predict")

        out = self.run_pipelines(X, None, "decision_function(X)", 
                                 resources=resources, pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)
        self.clean_pipelines_tmp()

        return out

    def fit_predict(self, X, y, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.run_pipelines(X, y, "fit_predict(X,y)",  resources=resources,
                                 pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)
        self.clean_pipelines_tmp()

        return out

    def wait_for_pipelines(self, running, numberToWait=None):

        if (numberToWait == None):
            numberToWait = len(running)

        finished = set()

        while len(finished) < numberToWait:
            for pipeline in running:
                if(not pipeline.is_running()):
                    finished.add(pipeline)
                    print(f"Finished pipeline '{pipeline.workflow_name}' ({pipeline.backend})")
            
            sleep(self.pooling_interval)

        return finished

    def config(self, resources=None, function_resources=None, concurrent_pipelines=None, namespace=None, tmpFolder=None, node_selector=None):
        
        if node_selector:
            self.node_selector = node_selector
        if namespace:
            self.namespace = namespace
        if tmpFolder:
            self.tmpFolder = tmpFolder
        if resources:
            self.resources = resources
        if function_resources:
            self.function_resources = function_resources
        if concurrent_pipelines:
            self.concurrent_pipelines = concurrent_pipelines