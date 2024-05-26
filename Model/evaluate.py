import pickle
import numpy as np
from sklearn.metrics import mean_absolute_error


def load_results(results_path):
    with open(results_path, 'rb') as fp:
        results = pickle.load(fp)
        return results


if __name__ == '__main__':
    results = load_results('../Results/moderate_model_results.pkl')
    x_test = results['x_test']
    y_test_pred = results['y_test_pred']
    y_test_real = results['y_test_real']

    home_team_score = y_test_real[:, 0]
    away_team_score = y_test_real[:, 1]
    avg_home_team_score = np.mean(home_team_score)
    avg_away_team_score = np.mean(away_team_score)
    avg_home_team_score_arr = np.full(home_team_score.shape, avg_home_team_score)
    avg_away_team_score_arr = np.full(away_team_score.shape, avg_away_team_score)
    home_baseline_mae = mean_absolute_error(home_team_score, avg_home_team_score_arr)
    away_baseline_mae = mean_absolute_error(away_team_score, avg_away_team_score_arr)
    baseline_mae = (home_baseline_mae + away_baseline_mae) / 2
    print(f'Actual MAE: {results["test_eval"][1]}')
    print(f'Baseline MAE: {baseline_mae}')

    # for i in range(x_test.shape[0]):
    #     print(i)
    #     print(f'Input: {x_test[i]}')
    #     print(f'Real: {y_test_real[i]}')
    #     print(f'Pred: {y_test_pred[i]}')
    #     print()
