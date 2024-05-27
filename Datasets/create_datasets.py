import pickle
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sklearn.preprocessing import StandardScaler
import pandas as pd
from database_helpers import connection_parameters
from dataset_queries import *
import time


def create_dataset(input_stats, output_stats, average_window, dataset_file_name, cursor,
                   include_win_percentage=True, include_days_since_last_game=True, games=None):
    # Store the dataset in two lists
    inputs = []
    outputs = []

    # Create SQL strings for selecting desired stats
    input_stat_columns = ", ".join(input_stats)
    input_avg_stat_columns = ", ".join([f"AVG({stat})" for stat in input_stats])
    output_home_stat_columns = ', '.join([f"home_stats.{stat}" for stat in output_stats])
    output_away_stat_columns = ', '.join([f"away_stats.{stat}" for stat in output_stats])

    # Get all the games
    if games is None:
        cursor.execute(all_games_with_stats.format(home_stat_columns=output_home_stat_columns,
                                                   away_stat_columns=output_away_stat_columns))
        games = cursor.fetchall()
    n_games = len(games)

    # For each game, create data point
    for i, game in enumerate(games):
        # Get game info
        game_date = game[0]
        home_team_name = game[1]
        away_team_name = game[2]
        print(f'{((i + 1) / n_games):.2%}: {game_date} {home_team_name} vs {away_team_name}')

        # Get output stats for both teams
        home_team_stats = game[3:3 + len(output_stats)]
        away_team_stats = game[3 + len(output_stats):]

        # Store inputs and outputs for game
        x = []
        y = [*home_team_stats, *away_team_stats]

        # Flag for if there is no history of previous games available for either team,
        no_data = False

        # Compute stats for both teams
        teams = [home_team_name, away_team_name]
        for j, team in enumerate(teams):
            # Get averages for all stats
            cursor.execute(team_stat_per_game.format(stat_columns=input_stat_columns,
                                                     aggregate_stat_columns=input_avg_stat_columns),
                           (team, True, team, True, game_date, average_window, team))
            stats_per_game = cursor.fetchone()

            cursor.execute(team_stat_per_game.format(stat_columns=input_stat_columns,
                                                     aggregate_stat_columns=input_avg_stat_columns),
                           (team, True if j == 0 else False, team, True if j == 1 else False, game_date,
                            average_window, team))
            stats_per_game_home_away = cursor.fetchone()

            cursor.execute(team_stat_conceded_per_game.format(stat_columns=input_stat_columns,
                                                              aggregate_stat_columns=input_avg_stat_columns),
                           (team, team, True, team, True, game_date, average_window))
            stats_conceded_per_game = cursor.fetchone()

            cursor.execute(team_stat_conceded_per_game.format(stat_columns=input_stat_columns,
                                                              aggregate_stat_columns=input_avg_stat_columns),
                           (team, team, True if j == 0 else False, team, True if j == 1 else False,
                            game_date, average_window))
            stats_conceded_per_game_home_away = cursor.fetchone()

            # Combine all results that (potentially) include multiple stats
            multiple_stats = [stats_per_game, stats_conceded_per_game,
                              stats_per_game_home_away, stats_conceded_per_game_home_away]

            # Convert stats to floats and handle missing data
            multiple_stats_floats = []
            for multiple_stat in multiple_stats:
                for stat in multiple_stat:
                    # If there is no data for stat, raise flag
                    if stat is None:
                        no_data = True
                        break

                    # Convert stat to float if it exists
                    multiple_stats_floats.append(float(stat))

                # Stop conversion if a stat was missing
                if no_data:
                    break

            # Stop creating data is no history of stats
            if no_data:
                break

            x.extend(multiple_stats_floats)

            # Get non-average, optional stats
            if include_win_percentage:
                cursor.execute(team_win_percentage, (team, team, team, True, team, True, game_date, average_window))
                win_percentage = cursor.fetchone()
                win_percentage = float(win_percentage[0]) if win_percentage is not None and win_percentage[
                    0] is not None else 0.0
                x.append(win_percentage)

                cursor.execute(team_win_percentage, (team, team, team, True if j == 0 else False, team,
                                                     True if j == 1 else False, game_date, average_window))
                win_percentage_home_away = cursor.fetchone()
                win_percentage_home_away = float(
                    win_percentage_home_away[0]) if win_percentage_home_away is not None and win_percentage_home_away[
                    0] is not None else 0.0
                x.append(win_percentage_home_away)

            if include_days_since_last_game:
                cursor.execute(team_days_since_last_game, (team, team, game_date, game_date))
                days_since_last_game = cursor.fetchone()
                days_since_last_game = days_since_last_game[0] if days_since_last_game is not None and \
                                                                  days_since_last_game[0] is not None else 0
                x.append(days_since_last_game)

        # Do not include data if no history of stats
        if no_data:
            continue

        # Add this games inputs and outputs to list of all games
        inputs.append(x)
        outputs.append(y)

    # Combine inputs and outputs
    dataset = [inputs, outputs]

    # Save dataset to file
    with open(f'Datasets/{dataset_file_name}', 'wb') as fp:
        pickle.dump(dataset, fp)


def create_dataset_scaled(input_stats, output_stats, average_window, dataset_filename, db_connection, games=None,
                          win_percentage=True, days_since_last_game=True, augment=True, scaler=None):
    # Store the dataset in two lists
    inputs = []
    outputs = []

    # Create SQL strings for selecting desired stats
    input_stat_columns = ", ".join([f"T.{stat} AS team_{stat}, O.{stat} AS opponent_{stat}" for stat in input_stats])
    input_avg_stat_columns = ", ".join(
        [f"AVG(CASE WHEN game_rank <= {average_window} THEN team_{stat} ELSE NULL END) AS team_{stat}, "
         f"AVG(CASE WHEN game_rank <= {average_window} THEN opponent_{stat} ELSE NULL END) AS opponent_{stat}"
         for stat in input_stats])
    if win_percentage:
        input_stat_columns += ', G.win'
        input_avg_stat_columns += f', AVG(CASE WHEN game_rank <= {average_window} THEN win ELSE NULL END) AS win'
    output_home_stat_columns = ', '.join([f"home_stats.{stat}" for stat in output_stats])
    output_away_stat_columns = ', '.join([f"away_stats.{stat}" for stat in output_stats])

    # Get all the games
    if games is None:
        games = pd.read_sql_query(all_games_with_stats.format(home_stat_columns=output_home_stat_columns,
                                                              away_stat_columns=output_away_stat_columns),
                                  db_connection)
    n_games = len(games)

    # For each game, create data point
    for i, game in games.iterrows():
        # Get game info
        game_date = game['game_date']
        home_team_name = game['home_team_name']
        away_team_name = game['away_team_name']
        print(f'{((i + 1) / n_games):.2%}: {game_date} {home_team_name} vs {away_team_name}')

        # Get output stats for both teams
        home_team_stats = game.iloc[3:3 + len(output_stats)]
        away_team_stats = game.iloc[3 + len(output_stats):]

        # Get stats for window of previous games for every team
        every_team_stats = pd.read_sql_query(every_team_every_thing.format(stat_columns=input_stat_columns,
                                                                           aggregate_stat_columns=input_avg_stat_columns),
                                             db_connection,
                                             params=(game_date, True, game_date, True, average_window))
        every_team_stats_home = pd.read_sql_query(every_team_every_thing.format(stat_columns=input_stat_columns,
                                                                                aggregate_stat_columns=input_avg_stat_columns),
                                                  db_connection,
                                                  params=(game_date, True, game_date, False, average_window))
        every_team_stats_away = pd.read_sql_query(every_team_every_thing.format(stat_columns=input_stat_columns,
                                                                                aggregate_stat_columns=input_avg_stat_columns),
                                                  db_connection,
                                                  params=(game_date, False, game_date, True, average_window))

        # Check if any dataframe is empty
        if every_team_stats.empty or every_team_stats_home.empty or every_team_stats_away.empty:
            continue

        # Check if any dataframe does not contain home team data
        if ((every_team_stats[every_team_stats['team_name'] == home_team_name].empty or
             every_team_stats_home[every_team_stats_home['team_name'] == home_team_name].empty) or
                every_team_stats_away[every_team_stats_away['team_name'] == home_team_name].empty):
            continue

        # Check if any dataframe does not contain away team data
        if ((every_team_stats[every_team_stats['team_name'] == away_team_name].empty or
             every_team_stats_home[every_team_stats_home['team_name'] == away_team_name].empty) or
                every_team_stats_away[every_team_stats_away['team_name'] == away_team_name].empty):
            continue

        # Set up scaler
        if scaler is None:
            scaler = StandardScaler()
        normalize_columns = list(every_team_stats.columns[1:1 + (len(input_stats) * 2)])
        base_columns = ['team_name', 'win']

        # Transform values column-wise using scaler
        normalized_every_team_stats = scaler.fit_transform(every_team_stats[normalize_columns])
        normalized_every_team_stats_home = scaler.fit_transform(every_team_stats_home[normalize_columns])
        normalized_every_team_stats_away = scaler.fit_transform(every_team_stats_away[normalize_columns])

        # Create new dataframes with scaled data. If augment is true, combine with raw data
        normalized_every_team_stats_df = pd.DataFrame(normalized_every_team_stats,
                                                      columns=['norm_' + col if augment else col for col in
                                                               normalize_columns])
        normalized_every_team_stats_home_df = pd.DataFrame(normalized_every_team_stats_home,
                                                           columns=['norm_' + col if augment else col for col in
                                                                    normalize_columns])
        normalized_every_team_stats_away_df = pd.DataFrame(normalized_every_team_stats_away,
                                                           columns=['norm_' + col if augment else col for col in
                                                                    normalize_columns])
        every_team_stats = pd.concat(
            [every_team_stats if augment else every_team_stats[base_columns], normalized_every_team_stats_df],
            axis=1)
        every_team_stats_home = pd.concat(
            [every_team_stats_home if augment else every_team_stats_home[base_columns],
             normalized_every_team_stats_home_df],
            axis=1)
        every_team_stats_away = pd.concat(
            [every_team_stats_away if augment else every_team_stats_away[base_columns],
             normalized_every_team_stats_away_df],
            axis=1)

        # Add days since last game
        if days_since_last_game:
            home_days_since_last_game = pd.read_sql_query(team_days_since_last_game,
                                                          db_connection,
                                                          params=(
                                                              home_team_name, home_team_name, game_date,
                                                              game_date)).iloc[
                0].to_list()
            away_days_since_last_game = pd.read_sql_query(team_days_since_last_game,
                                                          db_connection,
                                                          params=(
                                                              away_team_name, away_team_name, game_date,
                                                              game_date)).iloc[
                0].to_list()
        else:
            home_days_since_last_game = []
            away_days_since_last_game = []

        # Get stats for two teams involved in game
        home_stats = every_team_stats[every_team_stats['team_name'] == home_team_name].iloc[0, 1:].to_list()
        home_stats_home = every_team_stats_home[every_team_stats_home['team_name'] == home_team_name].iloc[0,
                          1:].to_list()
        away_stats = every_team_stats[every_team_stats['team_name'] == away_team_name].iloc[0, 1:].to_list()
        away_stats_away = every_team_stats_away[every_team_stats_away['team_name'] == away_team_name].iloc[0,
                          1:].to_list()

        # Store inputs and outputs for game
        x = [*home_stats, *home_stats_home, *home_days_since_last_game, *away_stats, *away_stats_away,
             *away_days_since_last_game]
        y = [*home_team_stats, *away_team_stats]

        # Add this games inputs and outputs to list of all games
        inputs.append(x)
        outputs.append(y)

    # Combine inputs and outputs
    dataset = [inputs, outputs]

    # Save dataset to file
    with open(f'Datasets/{dataset_filename}', 'wb') as fp:
        pickle.dump(dataset, fp)


if __name__ == '__main__':
    start_time = time.time()
    # Establish the database connection
    # db_connection = psycopg2.connect(**connection_parameters)
    # cursor = db_connection.cursor()

    simple_input_stats = ['points']
    moderate_input_stats = ['points', 'total_rebounds', 'assists', 'blocks', 'steals', 'turnovers']
    intermediate_input_stats = ['points', 'total_rebounds', 'assists', 'blocks', 'steals', 'turnovers',
                                'personal_fouls', 'true_shooting_percentage', 'effective_field_goal_percentage',
                                'three_point_attempt_rate', 'free_throw_rate', 'total_rebound_percentage',
                                'assist_percentage', 'steal_percentage', 'block_percentage', 'turnover_percentage',
                                'offensive_rating', 'defensive_rating']
    output_stats = ['points']
    # games = [('2024-05-24', 'Minnesota Timberwolves', 'Dallas Mavericks') + (0,) * 2 * len(output_stats)]
    # create_dataset(simple_input_stats, output_stats, 10, 'simple_dataset.pkl', cursor)

    # Close the cursor and connection
    # cursor.close()
    # db_connection.close()

    # Define your database connection details
    db_url = URL.create(
        drivername="postgresql+psycopg2",
        username=connection_parameters['user'],
        password=connection_parameters['password'],
        host=connection_parameters['host'],
        port=connection_parameters['port'],
        database=connection_parameters['dbname']
    )

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    create_dataset_scaled(intermediate_input_stats, output_stats, 10, 'intermediate_normalized_dataset.pkl', engine,
                          augment=False, days_since_last_game=True)
