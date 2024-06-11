import pickle
import random
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sklearn.preprocessing import StandardScaler
import pandas as pd
from database_helpers import connection_parameters
from dataset_queries import *


def create_dataset(input_stats, output_stats, average_window, dataset_file_name, cursor,
                   include_home_away_splits=True, include_win_percentage=True, include_days_since_last_game=True,
                   include_player_stats=False, player_input_stats=None, player_count_per_team=18,
                   include_player_home_away_splits=True,
                   include_betting_lines=False,
                   games=None):
    # Store the dataset in two lists
    inputs = []
    outputs = []

    # Store betting lines
    betting_lines = []

    # Create SQL strings for selecting desired stats
    input_stat_columns = ", ".join(input_stats)
    input_avg_stat_columns = ", ".join([f"AVG({stat})" for stat in input_stats])
    output_home_stat_columns = ', '.join([f"home_stats.{stat}" for stat in output_stats])
    output_away_stat_columns = ', '.join([f"away_stats.{stat}" for stat in output_stats])

    if include_player_stats:
        player_input_stat_columns = ", ".join([f"ps.{stat}" for stat in player_input_stats])
        player_avg_input_stat_columns = ", ".join([f"AVG({stat})" for stat in player_input_stats])

    # Get all the games
    if games is None and include_betting_lines:
        cursor.execute(all_games_with_stats_and_lines.format(home_stat_columns=output_home_stat_columns,
                                                             away_stat_columns=output_away_stat_columns))
        games = cursor.fetchall()
    elif games is None:
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
        away_team_stats = game[3 + len(output_stats):3 + 2 * len(output_stats)]

        # Get betting lines
        if include_betting_lines:
            spread = game[3 + 2 * len(output_stats)]
            total = game[3 + 2 * len(output_stats) + 1]
            betting_lines.append((spread, total))

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

            cursor.execute(team_stat_conceded_per_game.format(stat_columns=input_stat_columns,
                                                              aggregate_stat_columns=input_avg_stat_columns),
                           (team, team, True, team, True, game_date, average_window))
            stats_conceded_per_game = cursor.fetchone()

            # Get averages for all stats in home/away games
            if include_home_away_splits:
                cursor.execute(team_stat_per_game.format(stat_columns=input_stat_columns,
                                                         aggregate_stat_columns=input_avg_stat_columns),
                               (team, True if j == 0 else False, team, True if j == 1 else False, game_date,
                                average_window, team))
                stats_per_game_home_away = cursor.fetchone()

                cursor.execute(team_stat_conceded_per_game.format(stat_columns=input_stat_columns,
                                                                  aggregate_stat_columns=input_avg_stat_columns),
                               (team, team, True if j == 0 else False, team, True if j == 1 else False,
                                game_date, average_window))
                stats_conceded_per_game_home_away = cursor.fetchone()

                # Combine stats into list
                multiple_stats = [stats_per_game, stats_conceded_per_game,
                                  stats_per_game_home_away, stats_conceded_per_game_home_away]
            else:
                multiple_stats = [stats_per_game, stats_conceded_per_game]

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

            # Get win percentage
            if include_win_percentage:
                cursor.execute(team_win_percentage, (team, team, team, True, team, True, game_date, average_window))
                win_percentage = cursor.fetchone()
                win_percentage = float(win_percentage[0]) if win_percentage is not None and win_percentage[
                    0] is not None else 0.0
                x.append(win_percentage)

                # Get win percentage home/away
                if include_home_away_splits:
                    cursor.execute(team_win_percentage, (team, team, team, True if j == 0 else False, team,
                                                         True if j == 1 else False, game_date, average_window))
                    win_percentage_home_away = cursor.fetchone()
                    win_percentage_home_away = float(
                        win_percentage_home_away[0]) if win_percentage_home_away is not None and \
                                                        win_percentage_home_away[
                                                            0] is not None else 0.0
                    x.append(win_percentage_home_away)

            # Get days since last game
            if include_days_since_last_game:
                cursor.execute(team_days_since_last_game, (team, team, game_date, game_date))
                days_since_last_game = cursor.fetchone()
                days_since_last_game = days_since_last_game[0] if days_since_last_game is not None and \
                                                                  days_since_last_game[0] is not None else 0
                x.append(days_since_last_game)

            # Get player stats
            if include_player_stats:
                cursor.execute(players_stats_per_game.format(stat_columns=player_input_stat_columns,
                                                             aggregate_stat_columns=player_avg_input_stat_columns),
                               (game_date, home_team_name, away_team_name, j == 0, game_date, True, True,
                                average_window))
                players_stats = cursor.fetchall()

                # Create a dictionary from player name to stats
                players_stats_dict = {}
                for players_stat in players_stats:
                    players_stats_dict[players_stat[0]] = list(players_stat[1:])

                # Add home away splits for players
                if include_player_home_away_splits:
                    cursor.execute(players_stats_per_game.format(stat_columns=player_input_stat_columns,
                                                                 aggregate_stat_columns=player_avg_input_stat_columns),
                                   (game_date, home_team_name, away_team_name, j == 0, game_date, j == 0, j == 1,
                                    average_window))
                    players_stats_home_away = cursor.fetchall()

                    # Add home away splits to player's existing stats
                    for players_stat_home_away in players_stats_home_away:
                        players_stats_dict[players_stat_home_away[0]].extend(list(players_stat_home_away[1:]))

                    # If a player has yet to play at home/away, pad their stats with zeros
                    for player in players_stats_dict.keys():
                        if len(players_stats_dict[player]) < 2 * len(player_input_stats):
                            players_stats_dict[player].extend([0] * len(player_input_stats))

                # Pad number of players with zeros to ensure uniform dimension
                n_players = len(players_stats_dict.keys())
                n_player_stats = len(player_input_stats)
                for k in range(player_count_per_team - n_players):
                    players_stats_dict[f'padding_{k}'] = (0,) * n_player_stats * (
                        2 if include_player_home_away_splits else 1)

                # Randomly shuffle players to eliminate dependence on position within players vector
                players = list(players_stats_dict.keys())
                random.shuffle(players)

                # Convert stats to floats
                players_stats_floats = []
                for player in players:
                    for stat in players_stats_dict[player]:
                        players_stats_floats.append(float(stat))

                # Add player data to input vector
                x.extend(players_stats_floats)

        # Do not include data if no history of stats
        if no_data:
            continue

        # Add this games inputs and outputs to list of all games
        inputs.append(x)
        outputs.append(y)

    # Combine inputs and outputs
    dataset = [inputs, outputs]

    # Add betting lines to dataset
    if include_betting_lines:
        dataset.append(betting_lines)

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
    # Establish the database connection
    db_connection = psycopg2.connect(**connection_parameters)
    cursor = db_connection.cursor()

    simple_input_stats = ['points']
    simple_player_input_stats = ['minutes_played', 'points']
    moderate_input_stats = ['points', 'total_rebounds', 'assists', 'blocks', 'steals', 'turnovers']
    intermediate_input_stats = ['points', 'total_rebounds', 'assists', 'blocks', 'steals', 'turnovers',
                                'personal_fouls', 'true_shooting_percentage', 'effective_field_goal_percentage',
                                'three_point_attempt_rate', 'free_throw_rate', 'total_rebound_percentage',
                                'assist_percentage', 'steal_percentage', 'block_percentage', 'turnover_percentage',
                                'offensive_rating', 'defensive_rating']
    intermediate_2_input_stats = ['points', 'true_shooting_percentage', 'effective_field_goal_percentage',
                                  'three_point_attempt_rate', 'free_throw_rate', 'total_rebound_percentage',
                                  'assist_percentage', 'steal_percentage', 'block_percentage', 'turnover_percentage',
                                  'offensive_rating', 'defensive_rating']
    advanced_input_stats = ['points', 'true_shooting_percentage', 'effective_field_goal_percentage',
                            'three_point_attempt_rate', 'free_throw_rate', 'total_rebound_percentage',
                            'assist_percentage', 'steal_percentage', 'block_percentage', 'turnover_percentage',
                            'offensive_rating', 'defensive_rating']
    advanced_player_input_stats = ['minutes_played', 'points', 'usage_percentage', 'offensive_rating',
                                   'defensive_rating', 'box_plus_minus']
    complex_player_input_stats = ['minutes_played', 'points', 'total_rebounds', 'assists', 'blocks', 'steals',
                                  'turnovers', 'personal_fouls', 'true_shooting_percentage',
                                  'effective_field_goal_percentage', 'three_point_attempt_rate', 'free_throw_rate',
                                  'total_rebound_percentage', 'assist_percentage', 'steal_percentage',
                                  'block_percentage', 'turnover_percentage', 'usage_percentage', 'offensive_rating',
                                  'defensive_rating', 'box_plus_minus']
    output_stats = ['points']
    # games = [('2024-06-06', 'Boston Celtics', 'Dallas Mavericks') + (0,) * 2 * len(output_stats)]
    # create_dataset(intermediate_input_stats, output_stats, 10, 'predictions_2024-06-06.pkl', cursor,
    #                include_win_percentage=True, include_days_since_last_game=True, include_player_home_away_splits=True,
    #                include_player_stats=False, games=games)
    # create_dataset(intermediate_2_input_stats, output_stats, 10, 'intermediate_2_dataset.pkl', cursor,
    #                include_win_percentage=True, include_days_since_last_game=True, include_home_away_splits=True,
    #                include_player_stats=False)
    create_dataset(intermediate_input_stats, output_stats, 10, 'betting_intermediate_dataset.pkl', cursor,
                   include_win_percentage=True, include_days_since_last_game=True, include_home_away_splits=True,
                   include_player_stats=False, include_betting_lines=True)

    # Close the cursor and connection
    cursor.close()
    db_connection.close()

    # Define your database connection details
    # db_url = URL.create(
    #     drivername="postgresql+psycopg2",
    #     username=connection_parameters['user'],
    #     password=connection_parameters['password'],
    #     host=connection_parameters['host'],
    #     port=connection_parameters['port'],
    #     database=connection_parameters['dbname']
    # )
    #
    # # Create a SQLAlchemy engine
    # engine = create_engine(db_url)
    #
    # create_dataset_scaled(intermediate_input_stats, output_stats, 10, 'intermediate_normalized_dataset.pkl', engine,
    #                       augment=False, days_since_last_game=True)
