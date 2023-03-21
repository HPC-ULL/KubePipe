

from .pipeline import Pipeline

from kubernetes import client, config, watch

import dill as pickle
import socket

from time import sleep

from kubernetes.client.rest import ApiException

import uuid

from typing import Union, Dict

import threading

import sys

import zlib

class PipelineTCP(Pipeline):



    def initialize(
        self,
        *args,
        namespace: str = "argo",
        kube_api: Union[client.CoreV1Api, None] = None,
        registry_ip: Union[str, None] = None,
        use_gpu : Union[bool,None] = False,
        compress_data :  bool = False,
        **kwargs
    ):

        super().initialize(*args,namespace=namespace, kube_api=kube_api, registry_ip=registry_ip)

        self.compress_data = compress_data

        self.backend = "tcp"

        self.delim = b'`%$@`'

        self.buffer_size = 1024*4

        self.currentPort = None

        if(self.use_gpu is None):
            self.use_gpu = use_gpu

        return self


    def clean(self):
        pass

    def clean_tmp(self):
        pass

    def get_consumed(self):
        return self.energy

    def get_output(self):
        return self.output


    def manage_workflow(self, X, y, add_args, fit_data):
        self.wait_for_pod_status(self.workflow_name, "Running")

        host, port = self.get_lb_ip(self.service_name)

        socket = self.create_socket(host,port)

        pipe = self.funcs if fit_data else self.fitted_pipeline

        #Send input
        self.send_vars(socket, X,y, pipe, add_args)

        #Receive output
        output = self.receive_output(socket)



        if(isinstance(output,tuple)):
            self.output, self.energy = output
        else:
            self.output = output

        if(isinstance(self.output,bytes)):
            with open("/tmp/out.h5", "wb") as f:
                f.write(self.output)
            from tensorflow.keras.models import load_model
            self.output = load_model("/tmp/out.h5",compile = False)


        if(fit_data):
            self.fitted_pipeline = self.output

        return output


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

            self.kube_api.create_namespaced_service(
                namespace = self.namespace,
                body = servicebody
            )


    def send_var(self,s, var):
        data = pickle.dumps(var,byref=False) + self.delim

        if(self.compress_data):
            data = zlib.compress(data)

        s.sendall(data)


    def send_vars(self, s, x, y , pipe, add_args):
        self.send_var(s,x)
        self.send_var(s,y)
        self.send_var(s,pipe)
        self.send_var(s,add_args)


    def receive_output(self,s):
        data = b""

        while(True):
            bytes_read = s.recv(self.buffer_size)
            if not bytes_read:
                break
            
            data+=bytes_read

        s.close()        
        if(data == b""):
            print("Output is empty")
            return

        return pickle.loads(data)

    def get_lb_ip(self,serviceName):
        api_response = self.kube_api.read_namespaced_service_status(    
            name=serviceName,
            namespace=self.namespace)


        # while(api_response.status.load_balancer.ingress == None):
        #     api_response = self.kube_api.read_namespaced_service_status(    
        #     name=serviceName,
        #     namespace=self.namespace)

        # host = api_response.status.load_balancer.ingress[0].ip
        # if host == None: host = "localhost"


        return "verode18" , api_response.spec.ports[0].node_port


    def create_socket(self,host,port):

        while True:
            try:
                s = socket.create_connection(((host,port)))
                s.settimeout(1.0)
                ping = s.recv(10)
                if(ping != b"p"):
                    raise Exception("Not connected")

                s.settimeout(None)

                return s

            except Exception as e:
                sleep(0.1)

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

        self.buffer_size = sys.getsizeof(X)//20

       
        fit_data = operation.startswith("fit")

        if(self.resources != None):
            resources = self.resources

        if(self.node_selector != None):
            node_selector = self.node_selector

        run_name = str.lower(str(type(self.funcs[-1] ).__name__)) + "-" +  operation[:operation.index("(")]

        workflowname = f"pipeline-{run_name}-{self.id}-{str(uuid.uuid4())[:3]}"

        serviceName = workflowname+"-service"

        self.create_tcp_service("NodePort", {"pod" : workflowname}, serviceName, nodePort= self.currentPort)

        if(self.currentPort):
            self.currentPort+=1

        code = f"""

import socket

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 3000
              
BUFFER_SIZE = {self.buffer_size}

delim = {self.delim}

print("Initialize socket")
s = socket.socket()
s.bind((SERVER_HOST, SERVER_PORT))
s.listen(1)
print("Socket Listening")

import dill as pickle

from sklearn.pipeline import make_pipeline

import os

import zlib

import tensorflow as tf

physical_devices = tf.config.list_physical_devices('GPU')
try:
  tf.config.experimental.set_memory_growth(physical_devices[0], True)
except:
  # Invalid device or cannot modify virtual devices once initialized.
  pass

def receiveVars(s):
    total_vars = 4
    data = b""
    outputs = []
    while(len(outputs) < total_vars):
        bytes_read = s.recv(BUFFER_SIZE)
        data+=bytes_read
        if(delim in data):
            split_data = data.split(delim)
            split_data[0] = data+split_data[0]
            for i in range(len(split_data)-1):
                var = pickle.loads(split_data[i])
                if({self.compress_data}):
                    var = zlib.decompress(var)
                outputs.append(var)

            data = split_data[-1]
        
    return outputs

def sendOutPut(s, output):
    print("Sending output")
   
    try:
        from scikeras.wrappers import BaseWrapper

        if({fit_data} and isinstance(output[-1],BaseWrapper)):

            output[-1].model_.save("/tmp/out",save_format="h5")
            with open ("/tmp/out", "rb") as f:
                output = f.read()

    except ModuleNotFoundError:
        pass


    s.sendall(pickle.dumps(output))

client_socket, address = s.accept() 
print("Connected " + str(address))

print("Ping host")
client_socket.send(b"p")

def work():
    print("Receiving variables")
    X,y,funcs,add_args = receiveVars(client_socket)
    print("Variables received")

    if({fit_data}):
        pipe = make_pipeline(*funcs)
    else:
        pipe = funcs

    output = pipe.{operation}

    return output


if({measure_energy}):
    from pyeml import measure_function
    sendOutPut(client_socket, measure_function(work))
else:
    sendOutPut(client_socket, work())
    
s.close()

"""
        volumes = []
        volume_mounts = []

        if (measure_energy):
            volumes.append(client.V1Volume(
                name="vol", host_path=client.V1HostPathVolumeSource(path="/dev/cpu")))
            volume_mounts.append(client.V1VolumeMount(
                name="vol", mount_path="/dev/cpu"))

        command = ["python3", "-u", "-c", code]
        container = client.V1Container(
            name=f"{self.id}",
            image=self.image,
            command=command,
            resources=client.V1ResourceRequirements(**resources),
            volume_mounts=volume_mounts,
            security_context=client.V1SecurityContext(
                privileged=measure_energy)
        )

        spec = client.V1PodSpec(restart_policy="Never", containers=[
                                container], node_selector=node_selector, volumes=volumes)

        body = client.V1Job(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=workflowname,  labels = {"pod": workflowname, "app" : "kubepipe"}),
            spec=spec
        )

        api_response = self.kube_api.create_namespaced_pod(
            body=body,
            namespace=self.namespace)

        self.workflow_name = workflowname
        self.service_name = serviceName

        threading.Thread(target=self.manage_workflow, args=(X,y,additional_args,fit_data)).start()

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
                self.kube_api.delete_namespaced_pod(self.workflow_name, self.namespace, async_req = False)
                self.kube_api.delete_namespaced_service( self.service_name, self.namespace)
                return False

            elif (status == "Failed" or status == "Error"):
                raise Exception(f"Workflow '{self.workflow_name}' has failed")

        except Exception as e:
            if (e.__class__.__name__ == "ApiException" and e.status == 404):
                None
            else:
                raise e

        return True

    def wait_for_pod_status(self, pod_name, status):

        try:
            while True:
                api_response = self.kube_api.read_namespaced_pod_status(
                    name=pod_name,
                    namespace=self.namespace,)
                if api_response.status.phase == status:
                    return status

                elif api_response.status.phase == "Failed" or api_response.status.phase == "Error":
                    raise Exception("Pod " + pod_name + " ha fallado\nLogs: {}".format(
                        self.kube_api.read_namespaced_pod_log(name=pod_name, namespace=self.namespace)))

                else:
                    sleep(0.1)

        except ApiException as e:
            if( e.status == 404):
                pass
            else:
                raise e

        

   