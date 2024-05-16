import numpy as np
import pandas as pd
from tensorflow.keras import layers
from tensorflow.keras.models import Sequential
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from scikeras.wrappers import KerasRegressor
from kube_pipe.pipeline import PipelineKubernetes, PipelineArgo, PipelineTCP, PipelineHTTP
from kube_pipe import KubePipe 

import os
from kube_pipe.model_selection import KubeGridSearch

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' 

features = ['t2m', 'ssrd', 'd2m', 'day_of_year', 'u10','v10','sp', 'year']

used_features = features.copy()

def preprocessing(train_data_path, sequence_length = 30, days_to_predict = 3, scaler = MinMaxScaler()):

    def create_sequences(data, sequence_length):
        sequences = []
        for i in range(len(data) - sequence_length):
            sequences.append(data[i:(i + sequence_length)])
        return np.array(sequences)
    

    def date_to_day_of_year(df):
        dates = pd.to_datetime(df[['year', 'month', 'day']])
        return dates.apply(lambda x: x.timetuple().tm_yday)
    
    
    train_data = pd.read_csv(train_data_path)

    train_data = train_data[~((train_data['month'] == 2) & (train_data['day'] == 29))]

    train_data['day_of_year'] = date_to_day_of_year(train_data)

    date_features = ['date', 'month', 'day', 'year']
    float_features = [feature for feature in used_features if feature not in date_features]
    train_data[float_features] = train_data[float_features].astype(np.float32)

    # scaler.fit_transform(train_data[used_features])

    train_input_sequences = create_sequences(train_data[used_features], sequence_length)

    train_target_sequences = train_input_sequences[sequence_length:]

    train_target_sequences = np.array([seq[:days_to_predict] for seq in train_target_sequences])

    train_target_sequences = train_target_sequences[:, :, 0:1]

    train_input_sequences = train_input_sequences[:-sequence_length]

    train_input_sequences = np.array([x.flatten() for x in train_input_sequences])
        

    train_target_sequences = np.array([x.flatten() for x in train_target_sequences])
    return train_input_sequences, train_target_sequences

def build_model(sequence_length, num_features, output_length):
    from tensorflow.keras.models import Sequential
    from tensorflow.keras import layers

    model = Sequential()
    model.add(layers.Input(shape=(sequence_length * num_features,)))
    model.add(layers.Reshape((sequence_length, num_features)))
    model.add(layers.LSTM(128, return_sequences=True, unroll=True))
    model.add(layers.Dropout(0.2))
    model.add(layers.LSTM(64, return_sequences=False))
    model.add(layers.Dropout(0.2))
    model.add(layers.Dense(output_length))
    model.compile(optimizer='adam', loss='mse')
    return model


sequence_length = 30
days_to_predict = 3
scaler = MinMaxScaler()
num_features = len(used_features)
train_file_path = r"C:\Users\danis\Desktop\KubePipe\test_datasets\combined_era5_data_full_data_train_debug.csv"
test_file_path = r"C:\Users\danis\Desktop\KubePipe\test_datasets\combined_era5_data_full_data_test.csv"


X_train, y_train = preprocessing(train_file_path, sequence_length, days_to_predict)
X_test, y_test = preprocessing(test_file_path, sequence_length, days_to_predict)


epochs = 1
batch_size = 32


model = KerasRegressor(model=build_model, model__sequence_length=sequence_length, model__num_features=num_features, model__output_length=days_to_predict, optimizer='adam', loss='mse', epochs=epochs, batch_size=batch_size, shuffle=False)


param_grid = {
    'epochs' : [1],
    # 'optimizer__learning_rate': [0.001, 0.01, 0.1]
}


kube_config = {
    # "context": "docker-desktop",
    "context": "turingpi",
}

# Creación del objeto KubePipe
pipelines = KubePipe(
    # PipelineKubernetes(MinMaxScaler(),KubeGridSearch(model, param_grid), node_selector={"node-name": "kube04"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube01"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube02"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube03"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube04"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube05"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube06"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube07"}),
    PipelineKubernetes(MinMaxScaler(),model, node_selector={"kubernetes.io/hostname": "kube08"}),

    kube_config=kube_config,
    # registry_ip="host.docker.internal:5000",
    
)

for pipeline in pipelines.pipelines:
    print(pipeline)

pipelines.fit(X_train, y_train)


scaler = MinMaxScaler().fit(X_train)

rmses = []

for model in pipelines.models:
    predicted = model.predict(X_test)

    rmses.append(np.sqrt(np.mean((predicted - y_test) ** 2)))


print(f"El pipeline con mejores resultados es el {pipelines.pipelines[rmses.index(min(rmses))]} con un rmse de {min(rmses)}")