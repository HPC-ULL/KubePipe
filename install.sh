#Deploy argo
kubectl create ns argo
kubectl apply -n argo -f https://raw.githubusercontent.com/argoproj/argo-workflows/master/manifests/quick-start-postgres.yaml
kubectl patch svc minio -n argo -p '{"spec": {"type": "LoadBalancer", "externalIPs":["192.168.64.2"]}}'

kubectl patch svc argo-server -n argo -p '{"spec": {"type": "LoadBalancer", "externalIPs":["192.168.64.2"]}}'

kubectl patch svc minio -n argo -p '{"spec": {"type": "NodePort"}}'
kubectl patch svc argo-server -n argo -p '{"spec": {"type": "NodePort"}}'

kubectl config set-credentials cluster-admin --token=$(kubectl -n kubernetes-dashboard create token admin-user)
#Install dependencies
pip3 install -r requirements.txt
kubectl patch svc ml-pipelines-ui -n kubeflow -p '{"spec": {"type": "LoadBalancer", "externalIPs":["192.168.64.2"]}}'


kubectl patch svc minio -n argo -p '{"spec": {"type": "LoadBalancer", "externalIPs":["192.168.64.2"]}}'

kubectl patch svc prometheus-grafana -n prometheus -p '{"spec": {"type": "NodePort", "externalIPs":["192.168.64.2"]}}'

kubectl patch svc argo-server -n argo -p '{"spec": {"type": "LoadBalancer", "externalIPs":["192.168.64.2"]}}'


kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/1.0.0-beta4/nvidia-device-plugin.yml


kubectl run test -i --tty --image=alu0101040882/kubepipe:3.7.3-cuda-k40m   --restart=Never --rm -- python

docker run --rm -i -t --gpus all alu0101040882/kubepipe:3.7.3-cuda-k40m bash 