


from typing import Union, Dict

from kube_pipe.pipeline import PipelineKubernetes, PipelineArgo, PipelineTCP, PipelineHTTP

class Pipeline():

    def __new__(self,
                 *funcs,
                 pipeline_strategy: str = "kubernetes",
                 **kwargs):
        
        if pipeline_strategy == "kubernetes":
            return PipelineKubernetes(*funcs, **kwargs)
        elif pipeline_strategy == "argo":
            return PipelineArgo(*funcs, **kwargs)
        elif pipeline_strategy == "tcp":
            return PipelineTCP(*funcs, **kwargs)
        elif pipeline_strategy == "http":
            return PipelineHTTP(*funcs, **kwargs)
        else:
            raise ValueError(f"Pipeline strategy {pipeline_strategy} not supported")
            
   

