

from .pipeline_base import PipelineBase

from kubernetes import client, config, watch

import dill as pickle

from time import sleep
import time

from kubernetes.client.rest import ApiException

import uuid

from typing import Union, Dict

import threading

import sys

import zlib

import requests

class PipelineHTTP(PipelineBase):

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

        self.backend = "http"

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


        pipe = self.funcs if fit_data else self.fitted_pipeline

        #Send input
        # print("Connecting to pod", host, port)
        output = self.communicate_with_pod(host, port, X, y, pipe, add_args)


        if(isinstance(output,tuple)):
            self.output, self.energy = output
        else:
            self.output = output

        # if(isinstance(self.output,bytes)):
        #     with open("/tmp/out.h5", "wb") as f:
        #         f.write(self.output)
        #     from tensorflow.keras.models import load_model
        #     self.output = load_model("/tmp/out.h5",compile = False)


        if(fit_data):
            self.fitted_pipeline = self.output

        self.end_comunication = True


        return output

    def wait_for_server(self,host, port, timeout=60, interval=1):
        start_time = time.time()
        while True:
            try:
                server_url = f"http://{host}:{port}/check_status"
                response = requests.get(server_url)
                if response.status_code == 200:
                    return True
            except Exception as e:
                pass 

            if time.time() - start_time >= timeout:
                raise Exception("Timeout reached. Server did not become active.")  
            
            time.sleep(interval)

    def communicate_with_pod(self, host, port, X, y, funcs, add_args):
        try:
            if not self.wait_for_server(host, port):
                return None

            data = pickle.dumps([X, y, funcs, add_args])
            url = f"http://{host}:{port}"
            headers = {'Content-Type': 'application/octet-stream'}
            response = requests.post(url, data=data, headers=headers)
            
            if response.status_code == 200:
                output_data = response.content

                output = pickle.loads(output_data)
                return output
            else:
                print("Error:", response.status_code)
                return None
        except Exception as e:
            print("Error:", e)
            return None


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


  



    def get_lb_ip(self, service_name):
        ip = self.get_node_ip(self.get_random_node())
        # ip = "localhost"

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


    def run(
        self,
        X: any,
        y: any,
        operation: str = "fit(X,y,**add_args)",
        measure_energy: bool = False,
        resources: Union[Dict, None] = {},
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
from http.server import BaseHTTPRequestHandler, HTTPServer
import dill as pickle
from sklearn.pipeline import make_pipeline
import sys
class MyHTTPHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_sent = False

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            request_data = self.rfile.read(content_length)

            # Receiving variables
            print("Receiving variables")
            X, y, funcs, add_args = self.receive_vars(request_data)
            print("Variables received")

            if {fit_data}:
                pipe = make_pipeline(*funcs)
            else:
                pipe = funcs

            print(pipe)

            print("Executing operation {operation}")

            output = pipe.{operation}

            if {measure_energy}:
                output_data = self.send_output(measure_function(output))
            else:
                output_data = self.send_output(output)

            print("Operation executed")
            self.send_response(200)
            self.send_header('Content-type', 'application/octet-stream')
            self.end_headers()
            self.wfile.write(output_data)

            print("Output sent")

            self.output_sent = True
            sys.exit()
        except Exception as e:
            print("Error:", e)
            sys.exit()

    def do_GET(self):
        if self.path == '/check_status':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Server is active')
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not found')

    def receive_vars(self, request_data):
        data = pickle.loads(request_data)
        X, y, funcs, add_args = data
        return X, y, funcs, add_args

    def send_output(self, output):
        print("Sending output", output)
        # try:
        #     from scikeras.wrappers import BaseWrapper
        #     if isinstance(output, BaseWrapper):
        #         output.model_.save("/tmp/out", save_format='h5')
        #         with open("/tmp/out", "rb") as f:
        #             output = f.read()
        # except ModuleNotFoundError:
        #     pass

        return pickle.dumps(output)

class StoppableHTTPServer(HTTPServer):
    def run(self):
        print('Starting server...')
        self.serve_forever()

    def stop(self):
        print('Stopping server...')
        self.shutdown()

def run(server_class=StoppableHTTPServer, handler_class=MyHTTPHandler, port=3000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    try:
        httpd.run()
    finally:
        httpd.stop()

if __name__ == '__main__':
    run()
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

        

   