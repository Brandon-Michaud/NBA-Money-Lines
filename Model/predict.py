from tensorflow.keras.models import load_model
from data import *


def load_saved_model(model_path):
    return load_model(model_path)


def load_saved_models(model_path, rotations):
    models = []
    for rotation in rotations:
        models.append(load_saved_model(model_path.format(rotation)))
    return models


def get_average_prediction(inputs, outputs, models):
    predictions = np.empty((len(models), *outputs.shape))
    for i, model in enumerate(models):
        predictions[i] = model.predict(inputs, verbose=0)
    avg_predictions = np.mean(predictions, axis=0)
    return avg_predictions


if __name__ == '__main__':
    models = load_saved_models('../Results/intermediate_rotation_{}_layers_256_128_64_act_elu_lrate_1e-05_model.keras', range(10))
    inputs, outputs = load_dataset('../Datasets/predictions_2024-06-09.pkl')
