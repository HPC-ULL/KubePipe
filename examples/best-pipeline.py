from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.preprocessing import StandardScaler


from sklearn.model_selection import train_test_split
from sklearn import datasets

from  kube_pipe.kube_pipe_kubernetes import KubePipeKubernetes as KubePipe


iris = datasets.load_iris()

X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2)


# Creación del objeto KubePipe
pipelines = KubePipe(
    [StandardScaler(), AdaBoostClassifier()],
    [OneHotEncoder(), LogisticRegression()],
    [StandardScaler(), RandomForestClassifier()]

)

# Configurar los pipelines
pipelines.config(resources={"memory":  "100Mi", "cpu": 1}, 
                 function_resources = { 
                     AdaBoostClassifier()     : {"memory" :  "200Mi" },
                     LogisticRegression()     : {"memory" :  "200Mi" }, 
                     RandomForestClassifier() : {"memory" :  "200Mi" } },
                 concurrent_pipelines=1,
                 tmpFolder="tmp"
                 )

# Ajustar a los datos
pipelines.fit(X_train, y_train)

# Calcular puntuaciones
scores = pipelines.score(X_test, y_test)

# Encontrar el mejor pipeline
print(f"El pipeline con mejores resultados es el {scores.index(max(scores))}")