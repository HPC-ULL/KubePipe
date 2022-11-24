import socket
from time import sleep
import dill as pickle
import atexit


import uuid

from kubernetes import client, config

from kubernetes.client.rest import ApiException


import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)




class Kube_pipe():

    def __init__(self,*args,  namespace = "argo", **kwargs):

        super().__init__(*args)

        self.namespace = namespace


        self.delim = b'$#'

        config.load_kube_config()
        self.kubeApi = client.CoreV1Api()

        self.bufferSize = 1024

        self.currentPort = None


    def cleanWorkflows(self):
        if(self.workflows):
            for workflow in self.workflows:
                self.deleteWorkflow(workflow)



    def create_tcp_service(self,type,selector,serviceName, nodePort=None):

            servicespec=client.V1ServiceSpec(
                    type = type,
                    selector=selector,
                    ports=[client.V1ServicePort(
                        port=3000,
                        target_port=3000,
                        protocol="TCP",
                        node_port=nodePort
                )]
            )

            servicebody = client.V1Service(

                api_version="v1",
                kind="Service",
                metadata=client.V1ObjectMeta(
                    name=serviceName,
                    labels={ "app" : "kubepipe"}
                ),
                spec = servicespec              
            )

            self.kubeApi.create_namespaced_service(
                namespace = self.namespace,
                body = servicebody
            )

            print("\nLanzado el servicio: '" + serviceName + "'")


    def workflow(self, funcs, name, pipeId, operation="fit(X,y)", fitData=False, resources=None):

        if(resources == None):
            resources = self.kuberesources

        workflowname = f"pipeline-{name}-{self.id}-{pipeId}-{str(uuid.uuid4())[:3]}"
        serviceName = workflowname+"-service"

        self.create_tcp_service("NodePort", {"pod" : workflowname}, serviceName, nodePort= self.currentPort)

        if(self.currentPort):
            self.currentPort+=1

        code = f"""

import socket

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 3000
              
BUFFER_SIZE = {self.bufferSize}


delim = {self.delim}

print("Initialize socket")
s = socket.socket()
s.bind((SERVER_HOST, SERVER_PORT))
s.listen(1)
print("Socket Listening")


import dill as pickle

from sklearn.pipeline import make_pipeline

import os

def receiveVars(s):
    data = b""
    while(data.count(delim) < 3 ):
        bytes_read = s.recv(BUFFER_SIZE)
        data+=bytes_read
        print(data.count(delim))

    pickledVars = data.split(delim)

    return (pickle.loads(pickledVars[0]),pickle.loads(pickledVars[1]),pickle.loads(pickledVars[2]))
    

def sendOutPut(s, out):
    s.sendall(pickle.dumps(out))


client_socket, address = s.accept() 
print("Connected " + str(address))

print("Ping host")
client_socket.send(b"p")


print("Receiving variables")
X,y,funcs = receiveVars(client_socket)
print("Variables received")



if({fitData}):
    pipe = make_pipeline(*funcs)
else:
    pipe = funcs


output = pipe.{operation}


sendOutPut(client_socket, output)

s.close()

"""
        command = ["python3", "-u", "-c", code]
        container = client.V1Container(
            name=f"{pipeId}",
            image="alu0101040882/kubepipe:p3.9.13",
            command=command,
            resources=client.V1ResourceRequirements(limits=resources)

        )

        spec = client.V1PodSpec(restart_policy="Never", containers=[container])

        body = client.V1Job(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=workflowname,  labels = {"pod": workflowname, "app" : "kubepipe"}),
            spec=spec
        )

        api_response = self.kubeApi.create_namespaced_pod(
            body=body,
            namespace=self.namespace)
        print("\nLanzado el pipeline: '" + workflowname + "'")

        return { "podName" : workflowname, "serviceName" : serviceName, "funcs" : funcs}


    def waitForPodStatus(self, podName, status):

        try:
            while True:
                api_response = self.kubeApi.read_namespaced_pod_status(
                    name=podName,
                    namespace=self.namespace,)
                if api_response.status.phase == status:
                    print("Pod " + podName + " esta en estado " + status)
                    return status

                elif api_response.status.phase == "Failed" or api_response.status.phase == "Error":
                    raise Exception("Pod " + podName + " ha fallado\nLogs: {}".format(
                        self.kubeApi.read_namespaced_pod_log(name=podName, namespace=self.namespace)))

                else:
                    sleep(0.1)

        except ApiException as e:
            if( e.status == 404):
                pass
            else:
                raise e

    def sendVars(self, s, x, y , pipe):
        data = pickle.dumps(x, byref=False) + self.delim + pickle.dumps(y,byref=False) + self.delim + pickle.dumps(pipe,byref=False) + self.delim

        s.sendall(data)

    def receiveOutput(self,s):
        data = b""
        while(True):
            bytes_read = s.recv(self.bufferSize)
            if not bytes_read:
                break
            
            data+=bytes_read

        s.close()        
        if(data == b""):
            print("Output is empty")
            return

        return pickle.loads(data)

    def getLbIP(self,serviceName):
        api_response = self.kubeApi.read_namespaced_service_status(    
            name=serviceName,
            namespace=self.namespace)


        # while(api_response.status.load_balancer.ingress == None):
        #     api_response = self.kubeApi.read_namespaced_service_status(    
        #     name=serviceName,
        #     namespace=self.namespace)

        # host = api_response.status.load_balancer.ingress[0].ip
        # if host == None: host = "localhost"


        return "localhost" , api_response.spec.ports[0].node_port


    def createSocket(self,host,port):

        while True:
            try:
                s = socket.create_connection(((host,port)))
                s.settimeout(1.0)
                ping = s.recv(10)
                if(ping != b"p"):
                    raise Exception("Not connected")

                return s

            except Exception as e:
                sleep(0.1)

        
    def runPipelines(self, X, y, operation, name, resources=None, pipeIndex=None, applyToFuncs=None, output="output", outputPrefix="tmp", concurrent_pipelines=None, fitData=False):

        if pipeIndex == None:
            pipeIndex = range(len(self.pipelines))

        if concurrent_pipelines == None:
            if(self.concurrent_pipelines != None):
                concurrent_pipelines = self.concurrent_pipelines
            else:
                concurrent_pipelines = len(self.pipelines)

        self.workflows = []

        outputs = []

        for i, index in enumerate(pipeIndex):

            pipeline = self.pipelines[index]

            if applyToFuncs is not None and callable(applyToFuncs):
                funcs = applyToFuncs(funcs)

            self.workflows.append(self.workflow(pipeline["funcs"], f"{i}-{str.lower(str(type( pipeline['funcs'][-1] ).__name__))}-{name}-", pipeline["id"], operation=operation, fitData=fitData))

            # Check that no more than "concurrent_pipelines" are running at the same time, wait for a workflow to finish
            if(len(self.workflows) >= concurrent_pipelines):
                for workflow in self.workflows:
                    self.waitForPodStatus(workflow["podName"], "Running")
                    host, port = self.getLbIP(workflow["serviceName"])

                    workflow["host"] = host
                    workflow["port"] = port
                    workflow["socket"] = self.createSocket(host,port)

                    self.sendVars(workflow["socket"], X,y, workflow["funcs"])
  

                for workflow in list(self.workflows):
                    host, port = workflow["host"],workflow["port"]

                    outputs.append(self.receiveOutput(workflow["socket"]))
                    self.deleteWorkflow(workflow)
                    self.workflows.remove(workflow)

        return outputs


    def deleteWorkflow(self,workflow):
        self.kubeApi.delete_namespaced_pod( workflow["podName"], self.namespace)
        self.kubeApi.delete_namespaced_service( workflow["serviceName"], self.namespace)


    def fit(self, X, y, resources=None, concurrent_pipelines=None):
        output = self.runPipelines(X, y, "fit(X,y)", "fit", resources=resources,
                                   concurrent_pipelines=concurrent_pipelines, fitData=True)


        return output

    def score(self, X, y, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, y, "score(X,y)", "score",  resources=resources,
                                pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)


        return out

    def score_samples(self, X, resources=None, pipe_index=None, concurrent_pipelines=None):

        out = self.runPipelines(X, None, "score_samples(X)", "score_samples",   resources=resources,
                                pipeIndex=pipe_index, concurrent_pipelines=concurrent_pipelines)


        return out

    def transform(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, None, "transform(X)", "transform", resources=resources, pipeIndex=pipeIndex,
                                applyToFuncs=lambda f: f[:-1], output="X", concurrent_pipelines=concurrent_pipelines)


        return out

    def inverse_transform(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, None, "transform(X)", "inverse_transform", resources=resources, pipeIndex=pipeIndex,
                                applyToFuncs=lambda f: f[:-1][::-1], output="X", concurrent_pipelines=concurrent_pipelines)


        return out

    def predict_proba(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, None, "predict_proba(X)", "predict_proba",  resources=resources,
                                pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)


        return out

    def predict_log_proba(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, None, "predict_log_proba(X)", "predict_log_proba",
                                resources=resources, pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)


        return out

    def predict(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, None, "predict(X)", "predict",  resources=resources,
                                pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)


        return out

    def decision_function(self, X, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, None, "decision_function(X)", "decision_function",
                                resources=resources, pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)


        return out

    def fit_predict(self, X, y, resources=None, pipeIndex=None, concurrent_pipelines=None):

        out = self.runPipelines(X, y, "fit_predict(X,y)", "fit_predict",  fitData=True,
                                resources=resources, pipeIndex=pipeIndex, concurrent_pipelines=concurrent_pipelines)


        return out

    def config(self, resources=None, function_resources=None, concurrent_pipelines=None, namespace=None, tmpFolder=None, firstPort = None):

        if namespace:
            self.namespace = namespace
  
        if resources:
            self.kuberesources = resources
        if function_resources:
            self.function_resources = function_resources
        if concurrent_pipelines:
            self.concurrent_pipelines = concurrent_pipelines
        if firstPort:
            self.currentPort = firstPort

  