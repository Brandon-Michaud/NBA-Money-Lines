import pickle
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
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


def extract_metric(results, idx):
    metrics = np.empty(len(results))
    for i, result in enumerate(results):
        metrics[i] = result['test_eval'][idx]
    return metrics


def extract_metrics(results, mse_idx=0, mae_idx=1, r2_idx=2):
    mses = extract_metric(results, idx=mse_idx)
    maes = extract_metric(results, idx=mae_idx)
    r2s = extract_metric(results, idx=r2_idx)
    return mses, maes, r2s


def perform_paired_ttest(sample1, sample2, confidence=0.95):
    ttest = scipy.stats.ttest_rel(sample1, sample2)
    print('Performing paired t-test')
    print('Null hypothesis: Both samples come from the same distribution')
    print(f'Difference in mean: {np.mean(sample1) - np.mean(sample2)}')
    print(f'pvalue: {ttest.pvalue}')
    print(f'Minimum confidence: {confidence}')
    if (1 - confidence) > ttest.pvalue:
        print('Decision: Reject null hypothesis')
        print(f'We are at least {confidence * 100}% confident that the samples come from different distributions')
    else:
        print('Decision: Fail to reject null hypothesis')
        print(f'We are NOT at least {confidence * 100}% confident that the samples come from different distributions')


def find_baseline_prediction(real_values):
    avg_values = np.mean(real_values, axis=0)
    avg_values_arr = np.full(real_values.shape, avg_values)
    baseline_mae = mean_absolute_error(real_values, avg_values_arr)
    baseline_mse = mean_squared_error(real_values, avg_values_arr)
    return baseline_mse, baseline_mae


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

    # perform_paired_ttest(intermediate_cv_maes, intermediate_2_cv_maes, confidence=0.95)
    find_baseline_prediction(intermediate_2_cv_results[0]['test_real'])

    # make_residual_plot_spread(intermediate_2_cv_results[0])

    # intermediate_no_splits_results = load_cross_validation_results('../Results/intermediate_no_splits_rotation_{}_128_64_32_act_elu_lrate_1e-05_results.pkl', rotations)
    # intermediate_no_splits_maes = extract_maes(intermediate_no_splits_results)
    #
    # simple_players_results = load_cross_validation_results('../Results/simple_players_rotation_{}_256_128_64_act_elu_lrate_1e-05_results.pkl', rotations)
    # simple_players_maes = extract_maes(simple_players_results)
