import pickle
import numpy as np
from sklearn.metrics import mean_absolute_error
import scipy


def load_results(results_path):
    with open(results_path, 'rb') as fp:
        results = pickle.load(fp)
        return results


def load_cross_validation_results(results_path, rotations):
    results = []
    for i, rotation in enumerate(rotations):
        results.append(load_results(results_path.format(rotation)))
    return results


def extract_maes(results, idx=1):
    maes = np.empty(len(results))
    worst = -np.infty
    best = np.infty
    for i, result in enumerate(results):
        maes[i] = result['test_eval'][idx]
        if maes[i] < best:
            best = maes[i]
        if maes[i] > worst:
            worst = maes[i]
    return maes, best, worst, np.mean(maes)


if __name__ == '__main__':
    simple_results = load_cross_validation_results('../Results/simple_rotation_{}_16_8_4_act_elu_lrate_1e-05_results.pkl', range(10))
    simple_maes = extract_maes(simple_results)

    moderate_results = load_cross_validation_results('../Results/moderate_rotation_{}_64_32_16_act_elu_lrate_1e-05_results.pkl', range(10))
    moderate_maes = extract_maes(moderate_results)

    intermediate_results = load_cross_validation_results('../Results/intermediate_rotation_{}_256_128_64_act_elu_lrate_1e-05_results.pkl', range(10))
    intermediate_maes = extract_maes(intermediate_results)

    intermediate_2_results = load_cross_validation_results('../Results/intermediate_2_rotation_{}_128_64_32_act_elu_lrate_1e-05_results.pkl', range(10))
    intermediate_2_maes = extract_maes(intermediate_2_results)

    simple_no_splits_results = load_cross_validation_results('../Results/simple_no_splits_rotation_{}_16_8_4_act_elu_lrate_1e-05_results.pkl', range(10))
    simple_no_splits_maes = extract_maes(simple_no_splits_results)

    intermediate_no_splits_results = load_cross_validation_results('../Results/intermediate_no_splits_rotation_{}_128_64_32_act_elu_lrate_1e-05_results.pkl', range(10))
    intermediate_no_splits_maes = extract_maes(intermediate_no_splits_results)

    simple_players_results = load_cross_validation_results('../Results/simple_players_rotation_{}_256_128_64_act_elu_lrate_1e-05_results.pkl', range(10))
    simple_players_maes = extract_maes(simple_players_results)

    t_test = scipy.stats.ttest_rel(intermediate_maes[0], intermediate_2_maes[0])
    print(f'pvalue = {t_test.pvalue}')
    print(f'diff = {intermediate_maes[3] - intermediate_2_maes[3]}')

    # results = load_results('../Results/moderate_model_results.pkl')
    # x_test = results['x_test']
    # y_test_pred = results['y_test_pred']
    # y_test_real = results['y_test_real']
    #
    # home_team_score = y_test_real[:, 0]
    # away_team_score = y_test_real[:, 1]
    # avg_home_team_score = np.mean(home_team_score)
    # avg_away_team_score = np.mean(away_team_score)
    # avg_home_team_score_arr = np.full(home_team_score.shape, avg_home_team_score)
    # avg_away_team_score_arr = np.full(away_team_score.shape, avg_away_team_score)
    # home_baseline_mae = mean_absolute_error(home_team_score, avg_home_team_score_arr)
    # away_baseline_mae = mean_absolute_error(away_team_score, avg_away_team_score_arr)
    # baseline_mae = (home_baseline_mae + away_baseline_mae) / 2
    # print(f'Actual MAE: {results["test_eval"][1]}')
    # print(f'Baseline MAE: {baseline_mae}')

    # for i in range(x_test.shape[0]):
    #     print(i)
    #     print(f'Input: {x_test[i]}')
    #     print(f'Real: {y_test_real[i]}')
    #     print(f'Pred: {y_test_pred[i]}')
    #     print()
    # 8.705896377563477
    # 8.977946281433105
    # 8.842391681671142
    #
    # 8.979540824890137
    # 9.328161239624023
    # 9.127171802520753
