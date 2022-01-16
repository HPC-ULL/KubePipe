
from os import pipe
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn import datasets

from kube_pipe_scikit_artifacts import make_kube_pipeline


iris = datasets.load_iris()

X_train,X_test, y_train, y_test = train_test_split(iris.data,iris.target,test_size=0.2)


#Creación de los pipelines
pipeline = make_kube_pipeline([OneHotEncoder(handle_unknown="ignore"), LogisticRegression()],
                              [OneHotEncoder(handle_unknown="ignore"), RandomForestClassifier()])


#Decorador 1
""" @kubeconfig(resources= {"memory" :  "100Mi"})
def fit(*args,**kwargs):
    return pipeline.fit(*args,**kwargs)

model = fit(X_train,y_train)
 """

#Decorador 2
""" model = kubeconfig(resources= {"memory" :  "100Mi"})(pipeline.fit)(X_train,y_train) """


#Recursos en llamada al fit
""" model = pipeline.fit(X_train,y_train, resources = {"memory" :  "100Mi"}) """


#Recursos en pipeline.con
pipeline.config( resources = {"memory" :  "100Mi"}, function_resources = { LogisticRegression()     : {"memory" :  "200Mi"}, 
                                                                           RandomForestClassifier() : {"memory" :  "50Mi" } } )
                                                                      
model = pipeline.fit(X_train,y_train)

print("Precision del pipeline : {} %".format( model.score(X_test,y_test)))