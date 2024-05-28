import os
import pickle
import wandb
import socket
import time
import tensorflow as tf

from tensorflow.keras.utils import plot_model
from tensorflow import keras

from parser import *
from model import *
from data import *


def generate_fname(args):
    '''
    Generate a file path for results based on arguments
    :param args: Command line arguments
    :return: File path
    '''
    # Create file path based on experiment type and datasets
    kfold = f'_rotation_{args.rotation}' if args.kfold else ''
    model = '_'.join(f'{layer}' for layer in args.hidden) + f'_act_{args.hidden_activation}'
    dropout = f'_dropout_{args.dropout}' if args.dropout is not None else ''
    l1 = f'_dropout_{args.l1}' if args.l1 is not None else ''
    l2 = f'_dropout_{args.l2}' if args.l2 is not None else ''
    es = '_es' if args.es else ''

    return f'{args.exp_type}{kfold}', f'{args.results_path}/{args.exp_type}{kfold}_{model}_lrate_{args.lrate}_epochs_{args.epochs}{es}{dropout}{l1}{l2}'


def create_and_compile_model(args, n_inputs, n_outputs, train_epoch_size):
    '''
    Creates a model based on the given arguments
    :param args: Command line arguments
    :param train_epoch_size: Size of a training epoch
    :return: Model
    '''
    # Create learning rate
    if args.lrd:
        decay_steps = args.lrd_steps * (train_epoch_size / args.batch)
        learning_rate = tf.keras.optimizers.schedules.ExponentialDecay(args.lrate,
                                                                       decay_steps=decay_steps,
                                                                       decay_rate=args.lrd_rate,
                                                                       staircase=True)
    else:
        learning_rate = args.lrate

    # Create optimizer
    if args.opt == 'SGD':
        opt = tf.keras.optimizers.SGD(learning_rate=learning_rate, momentum=args.momentum, weight_decay=args.decay)
    elif args.opt == 'Adam':
        opt = tf.keras.optimizers.Adam(learning_rate=learning_rate)
    else:
        raise Exception('Unknown optimizer')

    # Create loss and metrics
    loss = tf.keras.losses.MeanSquaredError()
    metrics = [tf.keras.metrics.MeanAbsoluteError()]

    # Create model
    model = create_model(n_inputs=n_inputs,
                         n_outputs=n_outputs,
                         hidden_layers=args.hidden,
                         hidden_activation=args.hidden_activation,
                         output_activation=args.output_activation,
                         dropout=args.dropout,
                         batch_normalization=args.batch_normalization)

    # Compile model and return
    model.compile(loss=loss, optimizer=opt, metrics=metrics)
    return model


def execute_exp(args=None, multi_gpus=False):
    '''
    Execute experiment based on given arguments
    :param args: Command line arguments
    :param multi_gpus: Use multiple GPUs (boolean)
    :return: Trained model
    '''
    # Scale the batch size with the number of GPUs
    if multi_gpus > 1:
        args.batch = args.batch * multi_gpus

    # Load dataset
    if args.kfold:
        x_train, y_train, x_val, y_val, x_test, y_test = load_dataset_with_folds(args.dataset, args.folds,
                                                                                 args.train_folds, args.val_folds,
                                                                                 args.test_folds, args.rotation,
                                                                                 args.random_fold_seed)
    else:
        x_train, y_train, x_val, y_val, x_test, y_test = load_dataset_with_splits(args.dataset, args.train_prop,
                                                                                  args.val_prop, args.test_prop,
                                                                                  args.random_fold_seed)

    # Create model using the command line arguments
    if multi_gpus > 1:
        mirrored_strategy = tf.distribute.MirroredStrategy()
        with mirrored_strategy.scope():
            model = create_and_compile_model(args, x_train.shape[1], y_train.shape[1], x_train.shape[0])
    else:
        model = create_and_compile_model(args, x_train.shape[1], y_train.shape[1], x_train.shape[0])

    # Report model structure if verbosity is turned on
    if args.verbose >= 1:
        print(model.summary())

    # Output file base and pkl file
    label, fbase = generate_fname(args)
    print(fbase)

    # Plot the model
    if args.render:
        render_fname = '%s_model_plot.png' % fbase
        plot_model(model, to_file=render_fname, show_shapes=True, show_layer_names=True)

    # Abort experiment if indicated by arguments
    if args.nogo:
        print("NO GO")
        return

    # Start weights and biases
    if args.wandb:
        run = wandb.init(project=args.project,
                         name=label,
                         notes=fbase, config=vars(args))
        wandb.log({'hostname': socket.gethostname()})

    # Callbacks
    cbs = []
    if args.es:
        early_stopping_cb = keras.callbacks.EarlyStopping(patience=args.es_patience, restore_best_weights=True,
                                                          min_delta=args.es_min_delta, monitor=args.es_monitor)
        cbs.append(early_stopping_cb)
    if args.lra:
        reduce_lr_cb = keras.callbacks.ReduceLROnPlateau(monitor=args.lra_monitor, factor=args.lra_factor,
                                                         patience=args.lra_patience, min_delta=args.lra_min_delta)
        cbs.append(reduce_lr_cb)

    # Log training to Weights and Biases
    if args.wandb:
        wandb_metrics_cb = wandb.keras.WandbMetricsLogger()
        cbs.append(wandb_metrics_cb)

    # Train model
    start_time = time.time()
    history = model.fit(x=x_train,
                        y=y_train,
                        epochs=args.epochs,
                        batch_size=args.batch,
                        verbose=args.verbose >= 2,
                        validation_data=(x_val, y_val),
                        validation_steps=None,
                        callbacks=cbs)
    end_time = time.time()

    # Generate results data
    results = {
        'train_time': end_time - start_time
    }

    # Test set evaluation
    test_eval = model.evaluate(x=x_test,
                               y=y_test,
                               batch_size=args.batch,
                               verbose=args.verbose >= 2)
    results['test_eval'] = test_eval

    # Test set predictions
    test_pred = model.predict(x_test)
    results['x_test'] = x_test
    results['y_test_pred'] = test_pred
    results['y_test_real'] = y_test

    # Log results to Weights and Biases
    if args.wandb:
        wandb.log({'final_test_mean_squared_error': test_eval[0]})
        wandb.log({'final_test_mean_absolute_error': test_eval[1]})

    # Save results
    with open("%s_results.pkl" % fbase, "wb") as fp:
        pickle.dump(results, fp)

    # Save model
    if args.save_model:
        model.save("%s_model.keras" % fbase)

    # End Weights and Biases session
    if args.wandb:
        wandb.finish()

    return model


if __name__ == "__main__":
    # Parse and check incoming arguments
    parser = create_parser()
    args = parser.parse_args()

    # Turn off GPU
    if not args.gpu or "CUDA_VISIBLE_DEVICES" not in os.environ.keys():
        tf.config.set_visible_devices([], 'GPU')
        print('NO VISIBLE DEVICES!!!!')

    # GPU check
    visible_devices = tf.config.get_visible_devices('GPU')
    n_visible_devices = len(visible_devices)
    print('GPUS:', visible_devices)
    if n_visible_devices > 0:
        for device in visible_devices:
            tf.config.experimental.set_memory_growth(device, True)
        print('We have %d GPUs\n' % n_visible_devices)
    else:
        print('NO GPU')

    # Set number of threads, if it is specified
    if args.cpus_per_task is not None:
        tf.config.threading.set_intra_op_parallelism_threads(args.cpus_per_task)
        tf.config.threading.set_inter_op_parallelism_threads(args.cpus_per_task)

    # Execute experiment
    execute_exp(args, multi_gpus=n_visible_devices > 1)
