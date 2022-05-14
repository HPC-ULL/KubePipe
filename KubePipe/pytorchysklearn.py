from PytorchWrapper import PytorchWrapper

from kube_pipe_kubernetes import make_kube_pipeline

from torchvision import datasets

from PytorchWrapper import PytorchWrapper

from sklearn.preprocessing import StandardScaler

from sklearn.model_selection import train_test_split
from sklearn import datasets

import torch
import torch.nn.functional as F
import torch.nn as nn
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier



from sklearn.utils.estimator_checks import check_estimator


iris = datasets.load_iris()
X = iris['data']
y = iris['target']

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Split the data set into training and testing
X_train, X_test, y_train, y_test = train_test_split(
    X_scaled, y, test_size=0.2, random_state=2)


# Define model


class Model(nn.Module):
    def __init__(self, input_dim):
        super(Model, self).__init__()
        self.layer1 = nn.Linear(input_dim, 50)
        self.layer2 = nn.Linear(50, 50)
        self.layer3 = nn.Linear(50, 3)
        
    def forward(self, x):
        x = F.relu(self.layer1(x))
        x = F.relu(self.layer2(x))
        x = F.softmax(self.layer3(x), dim=1)
        return x


model     = Model(X_train.shape[1])
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
loss_fn   = nn.CrossEntropyLoss()


def train_fn(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset)
    model.train()
    for batch, (X, y) in enumerate(dataloader):

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 100 == 0:
            loss, current = loss.item(), batch * len(X)
            print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


pipe = make_kube_pipeline(                          
    
                          [StandardScaler(),PytorchWrapper(model, epochs = 1)],
                          [StandardScaler(),PytorchWrapper(model, epochs = 100)],
                          minio_ip = "localhost:9000")




# Configurar los pipelines
pipe.config(resources={"memory":  "100Mi",  "nvidia.com/gpu" : 1}
                 ) 



pipe.fit(X_train, y_train)

print(f"Precision: {pipe.score(X_test, y_test)}%")



