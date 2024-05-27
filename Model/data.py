import pickle
import numpy as np
from sklearn.model_selection import KFold


def load_dataset(dataset_file):
    with open(dataset_file, 'rb') as fp:
        dataset = pickle.load(fp)
        inputs = np.array(dataset[0])
        outputs = np.array(dataset[1])
        return inputs, outputs


def create_folds(inputs, outputs, total_folds, training_folds, validation_folds, testing_folds, rotation, seed=42):
    if training_folds < 0 or validation_folds < 0 or testing_folds < 0:
        raise Exception('Split folds must be non-negative')

    if training_folds + validation_folds + testing_folds != total_folds:
        raise Exception('Split folds must sum to total folds')

    kf = KFold(n_splits=total_folds, shuffle=True, random_state=seed)
    fold_indices = list(kf.split(inputs))

    # Rotate folds
    fold_indices = fold_indices[rotation:] + fold_indices[:rotation]

    training_indices = []
    validation_indices = []
    testing_indices = []

    for i in range(total_folds):
        if i < training_folds:
            training_indices.extend(fold_indices[i][1])
        elif i < training_folds + validation_folds:
            validation_indices.extend(fold_indices[i][1])
        else:
            testing_indices.extend(fold_indices[i][1])

    training_indices = np.array(training_indices)
    validation_indices = np.array(validation_indices)
    testing_indices = np.array(testing_indices)

    training_input = inputs[training_indices]
    training_output = outputs[training_indices]
    validation_input = inputs[validation_indices]
    validation_output = outputs[validation_indices]
    testing_input = inputs[testing_indices]
    testing_output = outputs[testing_indices]

    return training_input, training_output, validation_input, validation_output, testing_input, testing_output


def create_splits(inputs, outputs, training_prop, validation_prop, testing_prop):
    if training_prop < 0 or validation_prop < 0 or testing_prop < 0:
        raise Exception('Split proportions must be non-negative')

    if training_prop + validation_prop + testing_prop != 1:
        raise Exception('Split proportions must sum to 1')

    n_examples = len(inputs)
    n_training = int(n_examples * training_prop)
    n_validation = int(n_examples * validation_prop)

    indices = np.arange(n_examples)
    np.random.shuffle(indices)

    training_indices = indices[:n_training]
    validation_indices = indices[n_training:n_training+n_validation]
    testing_indices = indices[n_training+n_validation:]

    training_input = inputs[training_indices]
    training_output = outputs[training_indices]
    validation_input = inputs[validation_indices]
    validation_output = outputs[validation_indices]
    testing_input = inputs[testing_indices]
    testing_output = outputs[testing_indices]

    return training_input, training_output, validation_input, validation_output, testing_input, testing_output


def load_dataset_with_splits(dataset_file, training_prop, validation_prop, testing_prop):
    inputs, outputs = load_dataset(dataset_file)
    return create_splits(inputs, outputs, training_prop, validation_prop, testing_prop)


def load_dataset_with_folds(dataset_file, total_folds, training_folds, validation_folds, testing_folds, rotation, seed=42):
    inputs, outputs = load_dataset(dataset_file)
    return create_folds(inputs, outputs, total_folds, training_folds, validation_folds, testing_folds, rotation, seed=seed)\


if __name__ == '__main__':
    # x_train, y_train, x_val, y_val, x_test, y_test = load_dataset_with_splits('../Datasets/normalized_augmented_intermediate_dataset.pkl', 0.7, 0.1, 0.2)
    x_train, y_train, x_val, y_val, x_test, y_test = load_dataset_with_folds('../Datasets/simple_dataset.pkl', 10, 8, 1, 1, 0)
    # print(x_train.shape)
    # print(y_train.shape)
    # print(x_val.shape)
    # print(y_val.shape)
    # print(x_test.shape)
    # print(y_test.shape)
