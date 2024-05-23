import pickle
import psycopg2
from database_helpers import connection_parameters
from dataset_queries import *


def create_simple_dataset(average_window, cursor):
    # Store the dataset in two lists
    inputs = []
    outputs = []

    # Execute the query
    cursor.execute(all_games)
    games = cursor.fetchall()
    n_games = len(games)
    print(n_games)

    # For each game, create data point
    for i, game in enumerate(games):
        # Get game info
        game_date, home_team_name, away_team_name, home_team_score, away_team_score = game
        print(f'{((i + 1) / n_games):.2%}: {game_date} {home_team_name} vs {away_team_name}')

        # Get input features for home team
        cursor.execute(team_points_per_game, (home_team_name, game_date, average_window))
        result = cursor.fetchone()
        home_team_points_per_game = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_points_per_game_home_away, (home_team_name, True, game_date, average_window))
        result = cursor.fetchone()
        home_team_points_per_game_home = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_points_conceded_per_game, (home_team_name, home_team_name, home_team_name,
                                                       game_date, average_window))
        result = cursor.fetchone()
        home_team_points_conceded_per_game = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_points_conceded_per_game_home, (home_team_name, game_date, average_window))
        result = cursor.fetchone()
        home_team_points_conceded_per_game_home = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_win_percentage, (home_team_name, home_team_name, home_team_name, home_team_name, game_date,
                                             average_window))
        result = cursor.fetchone()
        home_team_win_percentage = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_win_percentage_home, (home_team_name, game_date, average_window))
        result = cursor.fetchone()
        home_team_win_percentage_home = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_days_since_last_game, (home_team_name, home_team_name, game_date, game_date))
        result = cursor.fetchone()
        home_team_days_since_last_game = result[0] if result is not None and result[0] is not None else 0

        # Get input features for away team
        cursor.execute(team_points_per_game, (away_team_name, game_date, average_window))
        result = cursor.fetchone()
        away_team_points_per_game = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_points_per_game_home_away, (away_team_name, False, game_date, average_window))
        result = cursor.fetchone()
        away_team_points_per_game_away = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_points_conceded_per_game, (away_team_name, away_team_name, away_team_name,
                                                       game_date, average_window))
        result = cursor.fetchone()
        away_team_points_conceded_per_game = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_points_conceded_per_game_away, (away_team_name, game_date, average_window))
        result = cursor.fetchone()
        away_team_points_conceded_per_game_away = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_win_percentage, (away_team_name, away_team_name, away_team_name, away_team_name, game_date,
                                             average_window))
        result = cursor.fetchone()
        away_team_win_percentage = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_win_percentage_away, (away_team_name, game_date, average_window))
        result = cursor.fetchone()
        away_team_win_percentage_away = float(result[0]) if result is not None and result[0] is not None else 0.0

        cursor.execute(team_days_since_last_game, (away_team_name, away_team_name, game_date, game_date))
        result = cursor.fetchone()
        away_team_days_since_last_game = result[0] if result is not None and result[0] is not None else 0

        # Create lists for inputs and outputs
        x = [home_team_points_per_game, home_team_points_per_game_home, home_team_points_conceded_per_game,
             home_team_points_conceded_per_game_home, home_team_win_percentage, home_team_win_percentage_home,
             home_team_days_since_last_game,
             away_team_points_per_game, away_team_points_per_game_away, away_team_points_conceded_per_game,
             away_team_points_conceded_per_game_away, away_team_win_percentage, away_team_win_percentage_away,
             away_team_days_since_last_game]
        y = [home_team_score, away_team_score]

        # Append game features to dataset
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

    create_simple_dataset(10, cursor)

    # Close the cursor and connection
    cursor.close()
    db_connection.close()
