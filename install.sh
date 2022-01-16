#Deploy argo
kubectl create ns argo
kubectl apply -n argo -f https://raw.githubusercontent.com/argoproj/argo-workflows/master/manifests/quick-start-postgres.yaml
kubectl patch svc argo-server -n argo -p '{"spec": {"type": "LoadBalancer"}}'
kubectl patch svc minio -n argo -p '{"spec": {"type": "LoadBalancer"}}'

#Install dependencies
pip3 install -r requirements.txt