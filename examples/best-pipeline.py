from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler


from sklearn.model_selection import train_test_split
from sklearn import datasets

from kube_pipe.pipeline import PipelineKubernetes, PipelineArgo, PipelineTCP, PipelineHTTP
from kube_pipe import KubePipe 


from kube_pipe.model_selection import KubeGridSearch
#kubectl delete all -l app=kubepipe -n kubepipe

# """
# kubectl delete svc minio private-repository-k8s -n kubepipe
# kubectl apply -f C:\Users\danis\Desktop\KubePipe\KubePipe\yamls

# """


iris = datasets.load_iris()

X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2)


param_grid_adaboost = {
    'n_estimators': [100, 150],
    'learning_rate': [0.01, 0.1, 1.0]
}

param_grid_minmax = {
    'feature_range': [(0, 1), (-1, 1)],
}


kube_config = {
    # "context": "docker-desktop",
    "context": "turingpi",
}

# scheduler = {
#     "nodes" : [{"node-name": "kube01"}, {"node-name": "kube02"}, {"node-name": "kube03"}, {"node-name": "kube04"}],
#     "strategy": "round-robin"
# }

# Creación del objeto KubePipe
pipelines = KubePipe(
    PipelineHTTP(KubeGridSearch(MinMaxScaler(), param_grid_minmax), KubeGridSearch(AdaBoostClassifier(), param_grid_adaboost)),

    kube_config=kube_config,
    # scheduler=scheduler,

    # registry_ip="host.docker.internal:5000",
     
)


# Ajustar a los datos
a = pipelines.fit(X_train, y_train)

# Calcular puntuaciones
scores = pipelines.score(X_test, y_test)

# Encontrar el mejor pipeline
for pipeline, score in zip(pipelines.pipelines, scores):
    print(f"El pipeline {pipeline} tiene un score de {score}")

print(f"El pipeline con mejores resultados es el {pipelines.pipelines[scores.index(max(scores))]} con un score de {max(scores)}")