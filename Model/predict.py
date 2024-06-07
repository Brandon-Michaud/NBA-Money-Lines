from tensorflow.keras.models import load_model
from data import *


def load_saved_model(model_path):
    return load_model(model_path)


if __name__ == '__main__':
    model = load_saved_model('../Results/intermediate_layers_256_128_64_act_elu_lrate_1e-05_model.keras')
    inputs, outputs = load_dataset('../Datasets/predictions_2024-06-06.pkl')
    num_features = inputs.shape[1]
    for x in inputs:
        x = x.reshape(1, num_features)
        predictions = model.predict(x)
        print(predictions)
