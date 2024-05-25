from tensorflow.keras.models import load_model
from data import *


def load_saved_model(model_path):
    return load_model('../Results/simple_model.keras')


if __name__ == '__main__':
    model = load_saved_model('../Results/simple_model.keras')
    inputs, outputs = load_dataset('../Datasets/simple_predictions_dataset.pkl')
    num_features = inputs.shape[1]
    for x in inputs:
        x = x.reshape(1, num_features)
        predictions = model.predict(x)
        print(predictions)
