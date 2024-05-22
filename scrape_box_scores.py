import requests
import time
import pickle
import traceback
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
from database_helpers import connection_parameters


# Define constants for indices in INSERT PlayerStats queries
player_num_general_info = 5
player_num_stats = 35
player_boolean_indices = [4]
player_int_indices = [1, 2, 4, 5, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
player_int_indices = [i + player_num_general_info for i in player_int_indices]
player_float_indices = [i for i in range(player_num_general_info, player_num_general_info + player_num_stats) if i not in player_int_indices and i not in player_boolean_indices]
player_minutes_played_index = 5
player_minutes_played_index_redundant = 25

# Define constants for indices in INSERT TeamStats queries
team_num_general_info = 5
team_num_stats = 31
team_name_index = 1
team_points_index = team_num_general_info + 17
team_boolean_indices = [4]
team_int_indices = [0, 1, 3, 4, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17]
team_int_indices = [i + team_num_general_info for i in team_int_indices]
team_float_indices = [i for i in range(team_num_general_info, team_num_general_info + team_num_stats) if i not in team_int_indices and i not in team_boolean_indices]
team_unneeded_indices = [5, 24, 25, 37, 40]


def insert_teams(db_connection, cursor, teams):
    try:
        # Query for inserting into Teams
        insert_query = """
        INSERT INTO Teams (team_name)
        VALUES (%s)
        ON CONFLICT (team_name) DO NOTHING
        """

        # Insert each team
        for team in teams:
            # Convert string to tuple
            data_to_insert = (team,)

            # Execute the insert query
            cursor.execute(insert_query, data_to_insert)

            # Commit the transaction
            db_connection.commit()

    # Catch errors
    except Exception as error:
        db_connection.rollback()
        raise Exception(error)


def insert_players(db_connection, cursor, players):
    try:
        # Query for inserting into Players
        insert_query = """
        INSERT INTO Players (player_name)
        VALUES (%s)
        ON CONFLICT (player_name) DO NOTHING
        """

        # Insert each player
        for player in players:
            # Convert string to tuple
            data_to_insert = (player,)

            # Execute the insert query
            cursor.execute(insert_query, data_to_insert)

            # Commit the transaction
            db_connection.commit()

    # Catch errors
    except Exception as error:
        db_connection.rollback()
        raise Exception(error)


def insert_game(db_connection, cursor, game):
    try:
        # Query for inserting into Games
        insert_query = """
        INSERT INTO Games (game_date, home_team_name, away_team_name, home_team_score, away_team_score)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Game tuple
        data_to_insert = game

        # Execute the insert query
        cursor.execute(insert_query, data_to_insert)

        # Commit the transaction
        db_connection.commit()

    # Catch errors
    except Exception as error:
        db_connection.rollback()
        raise Exception(error)


def insert_player_stats(db_connection, cursor, player_stats):
    # Query for inserting into PlayerStats
    insert_query = """
    INSERT INTO PlayerStats (game_date, player_name, home_team_name, away_team_name, home, minutes_played, field_goals_made, field_goals_attempted, field_goal_percentage, three_pointers_made, three_pointers_attempted, three_pointer_percentage, free_throws_made, free_throws_attempted, free_throw_percentage, offensive_rebounds, defensive_rebounds, total_rebounds, assists, steals, blocks, turnovers, personal_fouls, points, plus_minus, true_shooting_percentage, effective_field_goal_percentage, three_point_attempt_rate, free_throw_rate, offensive_rebound_percentage, defensive_rebound_percentage, total_rebound_percentage, assist_percentage, steal_percentage, block_percentage, turnover_percentage, usage_percentage, offensive_rating, defensive_rating, box_plus_minus)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Record errors
    errors = ''

    # Insert stats for each player
    for player_stat in player_stats:
        # Convert list of stats to tuple
        data_to_insert = tuple(player_stat)

        try:
            # Execute the insert query
            cursor.execute(insert_query, data_to_insert)

        # Catch errors
        except Exception as error:
            errors += str(error) + '\n' + traceback.format_exc() + '\n'
            db_connection.rollback()

        # Commit the transaction
        db_connection.commit()

    # Lift errors to wrapper try-except
    if errors != '':
        raise Exception(errors)


def insert_team_stats(db_connection, cursor, team_stats):
    # Query for inserting into TeamStats
    insert_query = """
    INSERT INTO TeamStats (game_date, team_name, home_team_name, away_team_name, home, field_goals_made, field_goals_attempted, field_goal_percentage, three_pointers_made, three_pointers_attempted, three_pointer_percentage, free_throws_made, free_throws_attempted, free_throw_percentage, offensive_rebounds, defensive_rebounds, total_rebounds, assists, steals, blocks, turnovers, personal_fouls, points, true_shooting_percentage, effective_field_goal_percentage, three_point_attempt_rate, free_throw_rate, offensive_rebound_percentage, defensive_rebound_percentage, total_rebound_percentage, assist_percentage, steal_percentage, block_percentage, turnover_percentage, offensive_rating, defensive_rating)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Record errors
    errors = ''

    # Insert stats for each team
    for team_stat in team_stats:
        # Convert list of stats to tuple
        data_to_insert = tuple(team_stat)

        try:
            # Execute the insert query
            cursor.execute(insert_query, data_to_insert)

        # Catch errors
        except Exception as error:
            errors += str(error) + '\n' + traceback.format_exc() + '\n'
            db_connection.rollback()

        # Commit the transaction
        db_connection.commit()

    # Lift errors to wrapper try-except
    if errors != '':
        raise Exception(errors)


# Make dictionaries where keys are players and values are box score stats from HTML tables
def make_player_dictionaries(tables, sql_date_str, away_team, home_team, home):
    player_stats = {}

    # Combine basic and advanced box scores
    for table in tables:
        # Loop over every player in table
        rows = table.find_all('tr')
        for row in rows:
            # Get player name
            player_name = row.find('th').get_text()

            # Create general game and player information
            general_info = [sql_date_str, player_name, home_team, away_team, home]

            # Get the player stats
            stats = row.find_all('td')

            # If the player played, add his stats to the dictionary
            if len(stats) > 1:
                stats = row.find_all('td')

                # If the player did not play for any reason other than a coach's decision, do not include him
                if len(stats) == 1:
                    continue

                # Get stats, filling missing columns with zeros
                row_data = list(cell.get_text() if cell.get_text() != '' else 0 for cell in stats)

                # If basic box score is already added, append advanced stats
                if player_name in player_stats:
                    player_stats[player_name] = player_stats[player_name] + row_data

                # If basic box score has not been added, add it with general info
                else:
                    player_stats[player_name] = general_info + row_data

            # If the player did not play, determine reason
            elif len(stats) == 1:
                # If player did not play (coach's decision), fill box score with general info and pad with zeros
                if stats[0].get_text() == 'Did Not Play':
                    if player_name not in player_stats:
                        row_data = general_info + [0] * (player_num_stats + 1)
                        player_stats[player_name] = row_data

    # Clean data
    for key in player_stats.keys():
        # Clean team stats information
        if key == 'Team Totals':
            # Remove unneeded values
            for i in reversed(team_unneeded_indices):
                del player_stats[key][i]

            # Change team name from "Team Totals"
            player_stats[key][team_name_index] = home_team if home else away_team

            # Convert strings to integers
            for i in team_int_indices:
                player_stats[key][i] = int(player_stats[key][i])

            # Convert strings to floats
            for i in team_float_indices:
                player_stats[key][i] = float(player_stats[key][i])

        # Clean player stats information
        else:
            # Remove redundant minutes played
            del player_stats[key][player_minutes_played_index_redundant]

            # Convert minutes played to floatvmi
            if isinstance(player_stats[key][player_minutes_played_index], str):
                minutes, seconds = map(int, player_stats[key][player_minutes_played_index].split(':'))
                total_minutes = minutes + seconds / 60
                player_stats[key][player_minutes_played_index] = total_minutes

            # Convert strings to integers
            for i in player_int_indices:
                player_stats[key][i] = int(player_stats[key][i])

            # Convert strings to floats
            for i in player_float_indices:
                player_stats[key][i] = float(player_stats[key][i])

    return player_stats


# Scrape a single box score from basketball reference
def scrape_box_score_bbref(html, db_connection, cursor):
    # Create parser
    soup = BeautifulSoup(html, "html.parser")

    # Remove quarter and half box score tables
    toggleable = soup.find_all('div', class_='toggleable')
    for div in toggleable:
        div.decompose()

    # Remove headers
    headers = soup.find_all('thead')
    for header in headers:
        header.decompose()
    headers = soup.find_all('tr', class_='thead')
    for header in headers:
        header.decompose()

    # Get team names
    scorebox = soup.find('div', class_='scorebox')
    teams = scorebox.find_all('strong')
    away_team = teams[0].find('a').get_text()
    home_team = teams[1].find('a').get_text()
    teams = [away_team, home_team]

    # Get date
    box = soup.find('h1').get_text()
    date_str = box.split(', ', 1)[1]
    date = datetime.strptime(date_str, '%B %d, %Y')
    sql_date_str = date.strftime('%Y-%m-%d')

    # Get tables
    tables = soup.find_all('table')
    away_tables = tables[:2]
    home_tables = tables[2:]

    # Get player stats
    away_player_stats = make_player_dictionaries(away_tables, sql_date_str, away_team, home_team, home=False)
    home_player_stats = make_player_dictionaries(home_tables, sql_date_str, away_team, home_team, home=True)

    # Get team stats
    away_team_stats = away_player_stats.pop('Team Totals')
    home_team_stats = home_player_stats.pop('Team Totals')

    # Combine home and away stats
    player_stats = {**away_player_stats, **home_player_stats}
    team_stats = [away_team_stats, home_team_stats]

    # Create game tuple
    game = (sql_date_str, home_team, away_team, home_team_stats[team_points_index], away_team_stats[team_points_index])

    # Insert data into database
    insert_teams(db_connection, cursor, teams)
    insert_players(db_connection, cursor, player_stats.keys())
    insert_game(db_connection, cursor, game)
    insert_player_stats(db_connection, cursor, player_stats.values())
    insert_team_stats(db_connection, cursor, team_stats)


if __name__ == '__main__':
    # Load links to box scores
    with open('bbref_box_score_links.pkl', "rb") as fp:
        links = pickle.load(fp)
        n_links = len(links)

        # Base of URLs for box scores
        base_url = 'https://www.basketball-reference.com'

        # Store failed box score links
        failed_links = {}

        # Establish the database connection
        db_connection = psycopg2.connect(**connection_parameters)
        cursor = db_connection.cursor()

        # Scrape box score from each link
        for i, link in enumerate(links):
            print(f'{((i + 1) / n_links):.2%}: {link}')

            # Get HTML of box score from link
            url = base_url + link
            data = requests.get(url)

            # Scrape box score and handle error if occurs
            try:
                scrape_box_score_bbref(data.text, db_connection, cursor)
            except Exception as error:
                print(f'Error occurred: {error}')
                traceback.print_exc()
                failed_links[link] = [error, traceback.format_exc()]

            # Sleep for 2 seconds to ensure <= 30 requests per minute (required by bbref)
            time.sleep(2)

        # Close the cursor and connection
        cursor.close()
        db_connection.close()

        # Save list of links to file for later use (and reuse)
        with open('bbref_failed_box_score_links.pkl', 'wb') as fp2:
            pickle.dump(failed_links, fp2)
