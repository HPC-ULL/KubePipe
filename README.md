# KubePipe

KubePipe is a tool to paralelize the execution of multiple Machine Learning pipelines in containers orchestated by Kubernetes.

## Installation
Install Argo Workflows
```bash
kubectl create ns argo
kubectl apply -n argo -f https://raw.githubusercontent.com/argoproj/argo-workflows/master/manifests/quick-start-postgres.yaml
kubectl patch svc minio -n argo -p '{"spec": {"type": "LoadBalancer"}}'
kubectl patch svc argo-server -n argo -p '{"spec": {"type": "LoadBalancer"}}'
```

Install KubePipe
```bash
pip install git+https://github.com/HPC-ULL/KubePipe
```

## Usage

```python
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.preprocessing import StandardScaler, OneHotEncoder

from kube_pipe.kube_pipe_kubernetes import KubePipeKubernetes as KubePipe


from sklearn.model_selection import train_test_split
from sklearn import datasets

iris = datasets.load_iris()

X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2)


pipelines = KubePipe(
    [StandardScaler(), AdaBoostClassifier()],
    [OneHotEncoder(), LogisticRegression()],
    [StandardScaler(), RandomForestClassifier()],
)


pipelines.fit(X_train, y_train)

scores = pipelines.score(X_test, y_test)

```


## License

[MIT](https://choosealicense.com/licenses/mit/)