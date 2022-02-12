
import pandas as pd
from sklearn.pipeline import make_pipeline

from sklearn import datasets

from kube_pipe_scikit_artifacts import Kube_pipe, make_kube_pipeline

import os

import time
import datetime

from sklearn.preprocessing import StandardScaler

from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis

import gc

import csv

workdir = f"{os.path.dirname(os.path.realpath(__file__))}/test-results"

test_samples = [100,1000,2000,3000,4000,5000,10000,20000,30000,40000,50000,100000,200000,300000,400000,500000]

NUMBER_OF_FEATURES = 5

NUMBER_OF_TEST = 1


clasifiers = [  

                [StandardScaler(), LogisticRegression()],
                [StandardScaler(), DecisionTreeClassifier()],
                [StandardScaler(), RandomForestClassifier()],
                [StandardScaler(), GaussianProcessClassifier(1.0 * RBF(1.0))],
                [StandardScaler(), SVC(gamma=2, C=1)]
            
            ]


kubepipelines = make_kube_pipeline(*clasifiers)


scikitPipelines = []


clasifierNames = []

for clasifier in clasifiers:
    scikitPipelines.append(make_pipeline(*clasifier))
    clasifierNames.append(str(type(clasifier[-1]).__name__))


def mean(arr):
    sum = 0

    for num in arr:
        sum+=num
    
    return sum/len(arr)

def test(pipelines,testTimes,X_train,y_train):
    times = []

    for i in range(testTimes):
        inicio = time.time()

        if(isinstance(pipelines,Kube_pipe)):
            pipelines.fit(X_train,y_train)
            pipelines.deleteTemporaryFiles()
        else:
            for pipeline in pipelines:
                pipeline.fit(X_train, y_train)

        fin = time.time()

        times.append(fin-inicio)

        str(datetime.timedelta(seconds=fin-inicio))
        print(times)

        return times

now = datetime.datetime.now().strftime("%d-%m_%H:%M")

os.mkdir(f"{workdir}/{now}")
os.mkdir(f"{workdir}/{now}/csv")
os.mkdir(f"{workdir}/{now}/plots")


with open(f"{workdir}/{now}/csv/times.csv", "a") as file:
    writer = csv.writer(file)
    writer.writerow(["Samples","Scikit","Kubernetes"])

with open(f"{workdir}/{now}/csv/speedup.csv", "a") as file:
    writer = csv.writer(file)
    writer.writerow(["Samples","Speed Up"])


scikitTimes = []
kubeTimes = []
speedUps = []

try:
    with open(f"{workdir}/{now}/summary.txt", "a") as f:
        f.write(f"Results of pipelines {clasifierNames}\n")

        for i , n_sample in enumerate(test_samples):
            X, y = datasets.make_classification(n_samples=n_sample,n_features=NUMBER_OF_FEATURES)
            f.write(f"{n_sample} samples:\n")

            scikitTimes.append(mean(test(scikitPipelines,NUMBER_OF_TEST,X,y)))
            f.write(f"Scikit Pipeline:    \t {str(datetime.timedelta(seconds=scikitTimes[i]))}  ({scikitTimes[i]} seconds)\n")

            kubeTimes.append(mean(test(kubepipelines,NUMBER_OF_TEST,X,y)))
            f.write(f"Kubernetes Pipeline:\t {str(datetime.timedelta(seconds=kubeTimes[i]))}  ({kubeTimes[i]} seconds)\n")

            speedUps.append(scikitTimes[i]/kubeTimes[i])
            f.write(f"Speedup:            \t {speedUps[i]}\n\n")

            print(f"samples:{n_sample}\nscikit: {scikitTimes[i]}\nkubernetes: {kubeTimes[i]}\nspeedup:{speedUps[i]}\n\n")

            with open(f"{workdir}/{now}/csv/times.csv", "a") as file:
                writer = csv.writer(file)
                writer.writerow([n_sample,scikitTimes[i],kubeTimes[i]])

            with open(f"{workdir}/{now}/csv/speedup.csv", "a") as file:
                writer = csv.writer(file)
                writer.writerow([n_sample,speedUps[i]])
        
            del X
            del y

            gc.collect()

            f.flush()
            os.fsync(f)

finally:

    import matplotlib.pyplot as plt
    import pandas as pd

    y_labels = []

    for sample in test_samples:
        y_labels.append(str(sample))

    plt.figure()
    plt.plot(y_labels[0:len(scikitTimes)], scikitTimes, label = "Scikit")
    plt.plot(y_labels[0:len(kubeTimes)], kubeTimes, label = "Kubernetes") 
    plt.xlabel("Nº Samples")
    plt.ylabel("Time (s)")
    plt.legend()
    plt.savefig(f"{workdir}/{now}/plots/times-plot.png")


    plt.figure()
    plt.plot(y_labels[0:len(speedUps)], speedUps, label = "speedups")
    plt.xlabel("Nº Samples")
    plt.ylabel("SpeedUp")
    plt.legend()
    plt.savefig(f"{workdir}/{now}/plots/speedup-plot.png")

    