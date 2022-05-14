import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets

from torchvision.transforms import ToTensor, Lambda, Compose
import matplotlib.pyplot as plt

from kube_pipe_pytorch import make_kube_pipeline
from PytorchWrapper import PytorchWrapper

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import  AdaBoostClassifier
from sklearn.preprocessing import StandardScaler

from sklearn.model_selection import train_test_split
from sklearn import datasets

from torch.utils.data import TensorDataset, DataLoader


iris = datasets.load_iris()

X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2)


""" # Download training data from open datasets.
training_data = datasets.FashionMNIST(
    root="data",
    train=True,
    download=True,
    transform=ToTensor(),
)

# Download test data from open datasets.
test_data = datasets.FashionMNIST(
    root="data",
    train=False,
    download=True,
    transform=ToTensor(),
) 


batch_size = 64

# Create data loaders.
train_dataloader = DataLoader(training_data, batch_size=batch_size)
test_dataloader = DataLoader(test_data, batch_size=batch_size)

for X, y in test_dataloader:
    print("Shape of X [N, C, H, W]: ", X.shape)
    print("Shape of y: ", y.shape, y.dtype)
    break
"""

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using {device} device")

# Define model


class LogisticRegression(torch.nn.Module):
     def __init__(self, input_dim, output_dim):
         super(LogisticRegression, self).__init__()
         self.linear = torch.nn.Linear(input_dim, output_dim)
     def forward(self, x):
         outputs = torch.sigmoid(self.linear(x))
         return outputs


model = LogisticRegression(4, 1).to(device)


loss_fn = nn.BCELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-3)


def train_fn(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset)
    model.train()
    for batch, (X, y) in enumerate(dataloader):
        X, y = X.to(device), y.to(device)

        # Compute prediction error
        pred = model(X.float())
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 100 == 0:
            loss, current = loss.item(), batch * len(X)
            print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def test_fn(dataloader, model, loss_fn):
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
            pred = model(X)
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()
    test_loss /= num_batches
    correct /= size
    print(
        f"Test Error: \n Accuracy: {(100*correct):>0.1f}%, Avg loss: {test_loss:>8f} \n")

    return correct


# init : funciones de train y test,  model, loss_fn, optimizer,
#
# start : datasets de train y test, optional epochs = 5

from torch import from_numpy, float32, long, float, LongTensor

X = from_numpy(X_train)
y = from_numpy(y_train)

X = X.type(torch.LongTensor)
y = y.type(torch.LongTensor)

train_dataset = TensorDataset(X,y)
dataloader  = DataLoader(train_dataset)


for X, y in dataloader:
    print("Shape of X [N, C, H, W]: ", X.shape)
    print("Shape of y: ", y.shape, y.dtype)
    break


epochs = 5
for t in range(epochs):
    print(f"Epoch {t+1}\n-------------------------------")
    train_fn(dataloader, model, loss_fn, optimizer)
print("Done!") 





""" 
pipeline = make_kube_pipeline(
    
#        [StandardScaler(), AdaBoostClassifier()],
#        [OneHotEncoder(), LogisticRegression()],
        [PytorchWrapper(loss_fn=loss_fn, train_fn=train_fn,
                        test_fn=test_fn, optimizer=optimizer, model=model)], 
        
        minio_ip="localhost:9000")


models = pipeline.fit(X_train, y_train, epochs=1)
 """