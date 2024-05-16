

from sklearn.utils.validation import check_X_y, check_array, check_is_fitted
from sklearn.base import BaseEstimator, ClassifierMixin



class TensorflowWrapper(BaseEstimator, ClassifierMixin):

    def __init__(self, model, loss_fn = None, optimizer = None, train_fn = None, epochs = 5, device = None):

        if(loss_fn is None):
            loss_fn = nn.CrossEntropyLoss()

        if(optimizer is None):
            optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

        if(train_fn is None):
            def function(dataloader, model, loss_fn, optimizer):
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

            train_fn = function
        self.device = device

        self.model = model
        self.optimizer = optimizer
        self.train_fn = train_fn

        self.loss_fn = loss_fn

        self.epochs = epochs

       

    def get_params(self, deep=True):
        return {"loss_fn": self.loss_fn, "model": self.model, "device" : self.device , "train_fn" : self.train_fn, "epochs" : self.epochs, "optimizer" : self.optimizer}

    def set_params(self, **parameters):
        for parameter, value in parameters.items():
            setattr(self, parameter, value)
            if(parameter == "device"):
                print("Set device to ", value)
                self.model.to(value)

        return self



    def toDataloader(self, X, y):

        if(not torch.is_tensor(X)): X = from_numpy(X).float()
        if(not torch.is_tensor(y)): y = from_numpy(y).long()

        if(self.device):
            X = X.to(self.device)
            y = y.to(self.device)
            self.model.to(self.device)

        train_dataset = TensorDataset(X,y)

        dataloader  = DataLoader(train_dataset)
      

        return dataloader
       
    def fit(self,X,y):

        #Comprobar si X e y se pueden utilizar
        X, y = check_X_y(X, y)

        # self.classes_ = unique_labels(y)
        # self.X_ = X
        # self.y_ = y

        #Transformar a un formato utilizable por pytorch

        print("Data")
        dataloader  = self.toDataloader(X,y)
        print("Data2")


        #Entrenar el modelo
        for t in range(self.epochs):
            print("Epoch ", t+1)
            self.train_fn(dataloader,  self.model, self.loss_fn, self.optimizer)

        return self

    
    def predict(self,X,y = None):
        
        # Check is fit had been called
        check_is_fitted(self)

        # Input validation
        X = check_array(X)

        dataloader = self.toDataloader(X,y)

        return self.model(dataloader)

    def setModelToDevice(self,device):
        self.model.to(device)


    def getModelParams(self):
        out = {}
        for name, param in self.model.named_parameters():
            out[name] = param

        return out

    def __getstate__(self):
        """Used for serializing instances"""

        state = self.__dict__.copy()

        self.model = self.model.to("cpu")


        model_scripted = jit.script(self.model)
        buffer = BytesIO()
        torch.jit.save(model_scripted, buffer)

        state["model"] = buffer

        return state

    def __setstate__(self, state):
        """Used for deserializing"""
        # restore the state which was picklable
        self.__dict__.update(state)

        self.model.seek(0)

        if(not torch.cuda.is_available()):
            print("GPU not available, default to CPU")
            self.device = "cpu"
            
        self.model = jit.load(self.model, map_location=torch.device("cpu"))


        print("Cuda:",torch.cuda.is_available(), sep = " ")


        if(self.device):
            self.model.to(self.device)
            print(f"Model set to {self.device}")


        self.optimizer.add_param_group({"params" : self.model.parameters()})
        


        #self.optimizer = self.optimizer.__class__(self.model.parameters(), lr=0.001)

        return self

    

    def score(self,X,y):
        model = self.model
        dataloader = self.toDataloader(X,y)
        loss_fn = self.loss_fn

        size = len(dataloader.dataset)
        num_batches = len(dataloader)
        model.eval()
        test_loss, correct = 0, 0
        with no_grad():
            for X, y in dataloader:
                pred = model(X)
                test_loss += loss_fn(pred, y).item()
                correct += (pred.argmax(1) == y).type(torch.float).sum().item()
        test_loss /= num_batches
        correct /= size
        #print(f"Test Error: \n Accuracy: {(100*correct):>0.1f}%, Avg loss: {test_loss:>8f} \n")

        return correct


    def setDevice(self,device):
        print("Device set to ", device)
        self.device = device