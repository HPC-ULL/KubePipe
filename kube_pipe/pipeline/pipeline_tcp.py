

from .pipeline_base import PipelineBase

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

class PipelineTCP(PipelineBase):


    def initialize(
        self,
        *args,
        namespace: str = "kubepipe",
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

        self.end_comunication = False
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

        self.end_comunication = False

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

    def get_lb_ip(self, service_name):


        
        ip = self.get_node_ip(self.get_random_node())

        while True:
            api_response = self.kube_api.read_namespaced_service(
                name=service_name,
                namespace=self.namespace
            )
            service_ports = api_response.spec.ports
            if service_ports:
                node_port = service_ports[0].node_port
                if node_port is not None:
                    return ip, node_port
            sleep(1) 


    def get_random_node(self):
        while True:
            api_response = self.kube_api.list_node()
            nodes = api_response.items
            if nodes:
                return nodes[0].metadata.name
            sleep(1)


    def get_node_ip(self, node_name):

        if 'docker-desktop' in node_name:
            return 'localhost'
        
        while True:
            api_response = self.kube_api.read_node(node_name)
            addresses = api_response.status.addresses
            for address in addresses:
                if address.type == "InternalIP":
                    return address.address
            sleep(1)



    def create_socket(self,host,port):
        while True:
            try:
                s = socket.create_connection(((host, port)))
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

        run_name = str.lower(str(type(self.funcs[-1] ).__name__)) + "-" +  operation[:operation.index("(")].replace("_","")

        workflowname = f"pipeline-{self.backend}-{run_name}-{self.id}-{str(uuid.uuid4())[:3]}"

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

def receiveVars(s, total_vars, BUFFER_SIZE, delim, compress_data):
    outputs = []
    while len(outputs) < total_vars:
        data = b""
        while delim not in data:
            bytes_read = s.recv(BUFFER_SIZE)
            if not bytes_read:
                break
            data += bytes_read
        
        if not data:
            break

        split_data = data.split(delim)
        for i in range(len(split_data) - 1):
            var = pickle.loads(split_data[i])
            if compress_data:
                var = zlib.decompress(var)
            outputs.append(var)
        delim_index = data.index(delim)
        data = data[delim_index + len(delim):]
        
    return outputs

def sendOutPut(s, output, fit_data = {fit_data}):
    print("Sending output")
    try:
        from scikeras.wrappers import BaseWrapper

        if fit_data and isinstance(output[-1], BaseWrapper):
            output[-1].model_.save("/tmp/out", save_format="h5")
            with open("/tmp/out", "rb") as f:
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
    X,y,funcs,add_args = receiveVars(client_socket, 4, BUFFER_SIZE, delim, {self.compress_data})
    print("Variables received")

    if({fit_data}):
        pipe = make_pipeline(*funcs)
    else:
        pipe = funcs

    print(pipe)

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
            self.node = workflow.spec.node_name

            if (status == "Succeeded" and self.end_comunication):
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

        

   