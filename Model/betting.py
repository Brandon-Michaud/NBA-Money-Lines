import pickle
import numpy as np
from predict import *


def load_dataset(dataset_file):
    with open(dataset_file, 'rb') as fp:
        dataset = pickle.load(fp)
        inputs = np.array(dataset[0])
        outputs = np.array(dataset[1])
        betting_lines = np.array(dataset[2])
        return inputs, outputs, betting_lines


def get_record_against_spread(predictions, spreads, results, threshold=0.5):
    wins = 0
    losses = 0
    pushes = 0
    no_bets = 0
    for i, prediction in enumerate(predictions):
        predicted_spread = prediction[1] - prediction[0]
        betting_spread = spreads[i]
        actual_spread = results[i][1] - results[i][0]
        place_bet = np.abs(betting_spread - predicted_spread) >= threshold

        if place_bet:
            if betting_spread == actual_spread:
                pushes += 1
            elif predicted_spread < betting_spread and actual_spread < betting_spread:
                wins += 1
            elif predicted_spread > betting_spread and actual_spread > betting_spread:
                wins += 1
            else:
                losses += 1
        else:
            no_bets += 1
    return wins, losses, pushes, no_bets, wins / (wins + losses)


def get_record_against_total(predictions, totals, results, threshold=0.5):
    wins = 0
    losses = 0
    pushes = 0
    no_bets = 0
    for i, prediction in enumerate(predictions):
        predicted_total = prediction[1] + prediction[0]
        betting_total = totals[i]
        actual_total = results[i][1] + results[i][0]
        place_bet = np.abs(betting_total - predicted_total) >= threshold

        if place_bet:
            if betting_total == actual_total:
                pushes += 1
            elif predicted_total < betting_total and actual_total < betting_total:
                wins += 1
            elif predicted_total > betting_total and actual_total > betting_total:
                wins += 1
            else:
                losses += 1
        else:
            no_bets += 1
    return wins, losses, pushes, no_bets, wins / (wins + losses)


if __name__ == '__main__':
    inputs, outputs, betting_lines = load_dataset('../Datasets/betting_intermediate_dataset.pkl')
    models = load_saved_models('../Results/intermediate_rotation_{}_layers_256_128_64_act_elu_lrate_1e-05_model.keras', range(10))
    predictions = get_average_prediction(inputs, outputs, models)
    print(get_record_against_spread(predictions, betting_lines[:, 0], outputs, threshold=7))
    print(get_record_against_total(predictions, betting_lines[:, 1], outputs, threshold=0.5))
