import os
from time import sleep
import yaml
import dill as pickle

import uuid

from abc import ABC, abstractmethod

import atexit



class KubePipeBase(ABC):

    def __init__(self,*args):

        self.id = str(uuid.uuid4())[:8]
          
        self.pipelines = []
        for arg in args:
            self.pipelines.append({
                "id" : str(uuid.uuid4())[:10],
                "funcs" : arg
            })

        self.tmpFolder = "tmp"

        self.kuberesources = None

        self.concurrent_pipelines = None

        self.function_resources = {}

        self.namespace = "argo"

        self.models = None
        
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
        




        