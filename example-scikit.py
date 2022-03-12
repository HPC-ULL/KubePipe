
from os import pipe
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier

from sklearn.model_selection import train_test_split
from sklearn import datasets

from kube_pipe_scikit_artifacts import make_kube_pipeline, Kube_pipe


iris = datasets.load_iris()

X_train,X_test, y_train, y_test = train_test_split(iris.data,iris.target,test_size=0.2)


#Creación de los pipelines
pipeline = Kube_pipe([OneHotEncoder(handle_unknown="ignore"), LogisticRegression()],
                     [OneHotEncoder(handle_unknown="ignore"), RandomForestClassifier()],

                     argo_ip = "https://172.31.1.31:30366",
                     minio_ip =  "172.31.1.31:30271"
                    )


#Recursos en pipeline.config
pipeline.config( resources = {"memory" :  "100Mi"}, concurrent_pipelines = 1,  function_resources = { LogisticRegression()     : {"memory" :  "200Mi"}, 
                                                                                                      RandomForestClassifier() : {"memory" :  "50Mi" } }, tmpFolder = "/home/bejeque/dsuarezl/.kubetmp" )
                                                                      
pipeline.fit(X_train,y_train, concurrent_pipelines=10)

"""

print("Precision del pipeline : {} %".format( pipeline.score(X_test,y_test) ))
 """

X_test = pipeline.transform(X_test)[1]
model = pipeline.getModel(1)

print(model.score(X_test,y_test))

