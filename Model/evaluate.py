import pickle
import numpy as np
from sklearn.metrics import mean_absolute_error
from data import *
import scipy
import matplotlib.pyplot as plt
import seaborn as sns


def load_results(results_path):
    with open(results_path, 'rb') as fp:
        results = pickle.load(fp)
        return results


def load_cross_validation_results(results_path, rotations):
    results = []
    for rotation in rotations:
        results.append(load_results(results_path.format(rotation)))
    return results


def extract_mses(results, idx=0):
    mses = np.empty(len(results))
    worst = -np.infty
    best = np.infty
    for i, result in enumerate(results):
        mses[i] = result['test_eval'][idx]
        if mses[i] < best:
            best = mses[i]
        if mses[i] > worst:
            worst = mses[i]
    return mses, best, worst, np.mean(mses)


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


def extract_r2_scores(results, idx=2):
    r2s = np.empty(len(results))
    worst = np.infty
    best = -np.infty
    for i, result in enumerate(results):
        r2s[i] = result['test_eval'][idx]
        if r2s[i] > best:
            best = r2s[i]
        if r2s[i] < worst:
            worst = r2s[i]
    return r2s, best, worst, np.mean(r2s)


def extract_metrics(results, mse_idx=0, mae_idx=1, r2_idx=2):
    mses = extract_mses(results, idx=mse_idx)
    maes = extract_maes(results, idx=mae_idx)
    r2s = extract_r2_scores(results, idx=r2_idx)
    return mses, maes, r2s


def make_residual_plot_total(results):
    test_real = results['test_real']
    test_pred = results['test_pred']
    test_real_total = np.sum(test_real, axis=1)
    test_pred_total = np.sum(test_pred, axis=1)
    test_residual_total = test_real_total - test_pred_total
    plt.figure(figsize=(10, 6))
    sns.residplot(x=test_pred_total, y=test_residual_total, lowess=True, line_kws={'color': 'red', 'lw': 1})
    plt.axhline(0, color='gray', linestyle='--', linewidth=1)
    plt.xlabel('Predicted Values')
    plt.ylabel('Residuals')
    plt.title('Residual Plot')
    plt.show()


def make_residual_plot_spread(results):
    test_real = results['test_real']
    test_pred = results['test_pred']
    test_real_spread = test_real[:, 0] - test_real[:, 1]
    test_pred_spread = test_pred[:, 0] - test_pred[:, 1]
    test_residual_spread = test_real_spread - test_pred_spread
    plt.figure(figsize=(10, 6))
    sns.residplot(x=test_pred_spread, y=test_residual_spread, lowess=True, line_kws={'color': 'red', 'lw': 1})
    plt.axhline(0, color='gray', linestyle='--', linewidth=1)
    plt.xlabel('Predicted Values')
    plt.ylabel('Residuals')
    plt.title('Residual Plot')
    plt.show()


if __name__ == '__main__':
    rotations = range(10)

    simple_cv_results = load_cross_validation_results('../Results/simple_rotation_{}_layers_16_8_4_act_elu_lrate_1e-05_results.pkl', rotations)
    simple_cv_mses, simple_cv_maes, simple_cv_r2s = extract_metrics(simple_cv_results)

    moderate_cv_results = load_cross_validation_results('../Results/moderate_rotation_{}_layers_64_32_16_act_elu_lrate_1e-05_results.pkl', rotations)
    moderate_cv_mses, moderate_cv_maes, moderate_cv_r2s = extract_metrics(moderate_cv_results)

    intermediate_cv_results = load_cross_validation_results('../Results/intermediate_rotation_{}_layers_256_128_64_act_elu_lrate_1e-05_results.pkl', rotations)
    intermediate_cv_mses, intermediate_cv_maes, intermediate_cv_r2s = extract_metrics(intermediate_cv_results)

    intermediate_2_cv_results = load_cross_validation_results('../Results/intermediate_2_rotation_{}_layers_256_128_64_act_elu_lrate_1e-05_results.pkl', rotations)
    intermediate_2_cv_mses, intermediate_2_cv_maes, intermediate_2_cv_r2s = extract_metrics(intermediate_2_cv_results)

    make_residual_plot_spread(intermediate_2_cv_results[0])

    # intermediate_no_splits_results = load_cross_validation_results('../Results/intermediate_no_splits_rotation_{}_128_64_32_act_elu_lrate_1e-05_results.pkl', rotations)
    # intermediate_no_splits_maes = extract_maes(intermediate_no_splits_results)
    #
    # simple_players_results = load_cross_validation_results('../Results/simple_players_rotation_{}_256_128_64_act_elu_lrate_1e-05_results.pkl', rotations)
    # simple_players_maes = extract_maes(simple_players_results)

    # t_test = scipy.stats.ttest_rel(intermediate_2_cv_maes[0], intermediate_cv_maes[0])
    # print(f'pvalue = {t_test.pvalue}')
    # print(f'diff = {intermediate_2_cv_maes[3] - intermediate_cv_maes[3]}')

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
