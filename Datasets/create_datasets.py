import pickle
import psycopg2
from database_helpers import connection_parameters
from dataset_queries import *


def create_dataset(input_stats, output_stats, average_window, cursor,
                   include_win_percentage=True, include_days_since_last_game=True):
    # Store the dataset in two lists
    inputs = []
    outputs = []

    # Create SQL strings for selecting desired stats
    input_stat_columns = ", ".join(input_stats)
    input_avg_stat_columns = ", ".join([f"AVG({stat})" for stat in input_stats])
    output_home_stat_columns = ', '.join([f"home_stats.{stat}" for stat in output_stats])
    output_away_stat_columns = ', '.join([f"away_stats.{stat}" for stat in output_stats])

    # Get all the games
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
        home_team_stats = game[3:3+len(output_stats)]
        away_team_stats = game[3+len(output_stats):]

        # Store inputs and outputs for game
        x = []
        y = [*home_team_stats, *away_team_stats]

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
            multiple_stats = [stats_per_game, stats_per_game_home_away,
                              stats_conceded_per_game, stats_conceded_per_game_home_away]

            # Convert stats to floats and handle missing data
            multiple_stats_floats = []
            for multiple_stat in multiple_stats:
                # If data is missing replace with zeros
                if multiple_stat is None:
                    multiple_stats_floats.append([0.0] * len(input_stats))

                # If data is present, convert to floats
                else:
                    for stat in multiple_stat:
                        multiple_stats_floats.append(float(stat) if stat is not None else 0.0)

            x.extend(multiple_stats_floats)

            # Get non-average, optional stats
            if include_win_percentage:
                cursor.execute(team_win_percentage, (team, team, team, True, team, True, game_date, average_window))
                win_percentage = cursor.fetchone()
                win_percentage = float(win_percentage[0]) if win_percentage is not None and win_percentage[0] is not None else 0.0
                x.append(win_percentage)

                cursor.execute(team_win_percentage, (team, team, team, True if j == 0 else False, team,
                                                     True if j == 1 else False, game_date, average_window))
                win_percentage_home_away = cursor.fetchone()
                win_percentage_home_away = float(win_percentage_home_away[0]) if win_percentage_home_away is not None and win_percentage_home_away[0] is not None else 0.0
                x.append(win_percentage_home_away)

            if include_days_since_last_game:
                cursor.execute(team_days_since_last_game, (team, team, game_date, game_date))
                days_since_last_game = cursor.fetchone()
                days_since_last_game = days_since_last_game[0] if days_since_last_game is not None and days_since_last_game[0] is not None else 0
                x.append(days_since_last_game)

        inputs.append(x)
        outputs.append(y)

    # Combine inputs and outputs
    dataset = [inputs, outputs]

    # Save dataset to file
    with open('simple_dataset.pkl', 'wb') as fp:
        pickle.dump(dataset, fp)


if __name__ == '__main__':
    # Establish the database connection
    db_connection = psycopg2.connect(**connection_parameters)
    cursor = db_connection.cursor()

    input_stats = ['points', 'total_rebounds']
    output_stats = ['points', 'total_rebounds']
    create_dataset(input_stats, output_stats, 2, cursor)

    # Close the cursor and connection
    cursor.close()
    db_connection.close()
