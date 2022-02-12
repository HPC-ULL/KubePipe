
import queue
import pandas as pd
from sklearn.pipeline import make_pipeline

from sklearn import datasets

from Kube_pipe import Kube_pipe, make_kube_pipeline

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

from multiprocessing import Process
from multiprocessing import Queue


import subprocess
import csv

import gc

import math

workdir = f"{os.path.dirname(os.path.realpath(__file__))}/test-results"

test_samples = test_samples = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000]


#test_samples = [10,10]

""" 
test_samples = []
for i in range(3,6):
    for j in range(1,11):
        number = j*pow(10,i)
        if(number not in test_samples):
            test_samples.append(number) 


"""


NUMBER_OF_FEATURES = 5

NUMBER_OF_TEST = 1



clasifiers = [  #40

                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],

                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],

                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],

                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],
                [StandardScaler(), AdaBoostClassifier()],

            ]


procsList = []
for i in range(0,int(math.log(len(clasifiers),2)+1 )):
    procsList.append(pow(2,i))

if(procsList[-1]!=len(clasifiers)):
    procsList.append(len(clasifiers))

print(procsList)

kubepipelines = make_kube_pipeline(*clasifiers)
kubepipelines.config(tmpFolder = "/home/bejeque/dsuarezl/.kubetmp")

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

def test(pipelines,testTimes,X_train,y_train, concurrent = None,queue = None):
    times = []

    for i in range(testTimes):

        if(isinstance(pipelines,Kube_pipe)):
            inicio = time.time()
            pipelines.fit(X_train,y_train, concurrent_pipelines = concurrent)

        else:
            inicio = time.time()
            for pipeline in pipelines:
                pipeline.fit(X_train, y_train)

        fin = time.time()

        times.append(fin-inicio)

        str(datetime.timedelta(seconds=fin-inicio))
    
    print(times)

    if(queue): queue.put(times)

    return times

now = datetime.datetime.now().strftime("%d-%m_%H:%M")

os.mkdir(f"{workdir}/{now}")
os.mkdir(f"{workdir}/{now}/csv")
os.mkdir(f"{workdir}/{now}/plots")


kubenames = ["Samples","Scikit"]
speednames = ["Samples"]
for i in range(1,len(clasifiers)+1):
    kubenames.append(f"Kubernetes-{i}concurrent")
    speednames.append(f"SpeedUp-{i}concurrent")


with open(f"{workdir}/{now}/csv/times.csv", "a") as file:
    writer = csv.writer(file)
    writer.writerow(kubenames)

with open(f"{workdir}/{now}/csv/speedup.csv", "a") as file:
    writer = csv.writer(file)
    writer.writerow(speednames)

del kubenames
del speednames

scikitTimes = []
kubeTimes = []
speedUps = []

OUTLIER_UMBRAL = 3

queue = Queue()

try:
    with open(f"{workdir}/{now}/summary.txt", "a") as f:
        f.write(f"Results of pipelines (concurrent) {clasifierNames}\n")

        for i , n_sample in enumerate(test_samples):
            X, y = datasets.make_classification(n_samples=n_sample,n_features=NUMBER_OF_FEATURES)

            f.write(f"{n_sample} samples:\n")

            
            p = Process(target=test, args=(scikitPipelines,NUMBER_OF_TEST,X,y), kwargs={"queue" : queue})
            p.start()
            p.join()

            print("Launched scikit test")
            scikitTimes.append(mean(queue.get()))
            f.write(f"Scikit Pipeline:    \t {scikitTimes[-1]} seconds\n")

            kubeTimes.append([])
            speedUps.append([])

            for conc in procsList:
                print(f"Launched kubernetes test {conc} concurrents")

                kubetime = mean(test(kubepipelines,NUMBER_OF_TEST,X,y,concurrent = conc))

                while(conc > 2 and (kubetime - kubeTimes[i][-1]) > OUTLIER_UMBRAL):
                    kubetime = mean(test(kubepipelines,NUMBER_OF_TEST,X,y,concurrent = conc))

                kubeTimes[i].append(kubetime)
                
                speedUps[i].append(scikitTimes[i]/kubeTimes[i][-1])

                print("\n\n")
                
            print(f"Results of {n_sample}:\nKubernetes: {kubeTimes[i]}\nScikit: {scikitTimes[i]}\n")
            f.write(f"Kubernetes Pipeline:\t {kubeTimes[i]} seconds\n")

            f.write(f"Speedup:            \t {speedUps[i]}\n")
                
            with open(f"{workdir}/{now}/csv/times.csv", "a") as file:
                writer = csv.writer(file)
                writer.writerow([n_sample,scikitTimes[i]]+kubeTimes[i])

            with open(f"{workdir}/{now}/csv/speedup.csv", "a") as file:
                writer = csv.writer(file)
                writer.writerow([n_sample]+speedUps[i])
        
            del X
            del y

            gc.collect()

            f.flush()
            os.fsync(f)

            print(f"samples:{n_sample}\nscikit: {scikitTimes[i]}\nkubernetes: {kubeTimes[i]}\nspeedup:{speedUps[i]}\n\n")

finally:

    import matplotlib.pyplot as plt
    import pandas as pd

    y_labels = []


    kube_proc_times = []

    speedups_proc = [] 

    for i in range(1,len(clasifiers)+1):
        kube_proc_times.append([])
        speedups_proc.append([])

    for i in range(len(kubeTimes)):

        for j in range(len(kubeTimes[i])):
            kube_proc_times[j].append(kubeTimes[i][j])
            speedups_proc[j].append(speedUps[i][j])


    for sample in test_samples:
        y_labels.append(str(sample))


    plt.figure()
    plt.plot(y_labels[0:len(scikitTimes)], scikitTimes, label = "Scikit")

    for i, times in enumerate(kube_proc_times):
        plt.plot(y_labels[0:len(times)], times, label = f"Kubernetes-{i}concurrent")

    plt.xlabel("Nº Samples")
    plt.ylabel("Time (s)")
    plt.legend()
    plt.savefig(f"{workdir}/{now}/plots/times-plot.png")


    plt.figure()
    for i, speedup in enumerate(speedups_proc):
        plt.plot(y_labels[0:len(speedup)], speedup, label = f"speedup-{i}concurrent")

    plt.xlabel("Nº Samples")
    plt.ylabel("SpeedUp")
    plt.legend()
    plt.savefig(f"{workdir}/{now}/plots/speedup-plot.png")

    
