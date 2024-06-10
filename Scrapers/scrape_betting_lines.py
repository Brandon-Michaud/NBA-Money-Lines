import time
import pickle
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import psycopg2
from database_helpers import connection_parameters
from mappings import abbreviation_to_name_2 as abbreviation_to_name, month_abbreviation_to_number


def scrape_betting_lines_rotowire(rows_filename):
    # URL to scrape data from
    url = 'https://www.rotowire.com/betting/nba/archive.php'

    # Set up Selenium
    s = Service("/Users/brandonmichaud/Projects/chromedriver")
    driver = webdriver.Chrome(service=s)

    # Get website and let data load
    driver.get(url)
    time.sleep(5)

    # Locate body of table
    container = driver.find_element(By.XPATH, "//div[@class='webix_ss_body']")

    # Click on first row
    actions = ActionChains(driver)
    first_element = container.find_element(By.XPATH, ".//div[@column='0']/div[@class='webix_cell']")
    actions.move_to_element(first_element).click().perform()

    # Keep tack of unique rows
    rows = []
    counter = 0

    # Months that occur in the year x + 1 for the x season
    add_year_months = set(range(1, 10))

    # Keep scrolling through table rows
    while True:
        # Get the HTML of the website
        html = container.get_attribute('outerHTML')

        # Set up parser for data
        soup = BeautifulSoup(html, "html.parser")
        body = soup.find('div', class_='webix_ss_body')

        # Find each column of table
        game_column = body.find('div', {'column': '0'})
        tipoff_column = body.find('div', {'column': '1'})
        season_column = body.find('div', {'column': '2'})
        total_column = body.find('div', {'column': '4'})
        spread_column = body.find('div', {'column': '5'})

        # Find each row in each column
        games = game_column.find_all('div', class_='webix_cell')
        tipoffs = tipoff_column.find_all('div', class_='webix_cell')
        seasons = season_column.find_all('div', class_='webix_cell')
        totals = total_column.find_all('div', class_='webix_cell')
        spreads = spread_column.find_all('div', class_='webix_cell')

        # Loop over each row of data
        temp_rows = []
        new_data = False
        for i in range(len(games)):
            # Get the home team and away team
            game_text = games[i].text
            teams = game_text.split(' @ ')
            home_team_name = abbreviation_to_name[teams[1]]
            away_team_name = abbreviation_to_name[teams[0]]

            # Get the game date
            tipoff_text = tipoffs[i].text
            tipoff_date = tipoff_text.split(' ', 2)[:2]
            month = month_abbreviation_to_number[tipoff_date[0]]
            day = int(tipoff_date[1])
            season = int(seasons[i].text)
            year = season + 1 if month in add_year_months else season
            game_date = f'{year}-{month:02}-{day:02}'

            # If total or spread not available, move on to next row
            if totals[i].text == '' or spreads[i].text == '':
                # Indicate that it is possible that new data was seen, but it was empty data
                new_data = True
                continue

            # Get total and spread for the game
            total = float(totals[i].text)
            spread = float(spreads[i].text)

            # Create a row tuple of data for this game
            row = (game_date, home_team_name, away_team_name, spread, total)
            temp_rows.append(row)

        # If a new row of data appeared, add it to set
        for row in temp_rows:
            if row not in rows:
                rows.append(row)
                print(row)
                new_data = True

        # If new data appear or we just started scraping continue
        if not new_data and counter >= 20:
            break

        # Click down to load the next row
        actions.send_keys(Keys.DOWN).perform()

        counter += 1
    driver.quit()

    # Save rows to file
    with open(rows_filename, 'wb') as fp:
        pickle.dump(rows, fp)


def insert_betting_line(db_connection, cursor, betting_line):
    try:
        # Query for inserting into BettingLines
        insert_query = """
        INSERT INTO BettingLines (game_date, home_team_name, away_team_name, spread, total)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Betting line tuple
        data_to_insert = betting_line

        # Execute the insert query
        cursor.execute(insert_query, data_to_insert)

        # Commit the transaction
        db_connection.commit()

    # Catch errors
    except Exception as error:
        db_connection.rollback()
        raise Exception(error)


def add_betting_lines_to_database(data_filename, db_connection, cursor, failed_filename):
    # Open the list of betting lines
    with open(data_filename, 'rb') as fp:
        betting_lines = pickle.load(fp)
        n_lines = len(betting_lines)

        # Keep track of betting lines that fail being inserted into database
        failed_betting_lines = {}

        # Add each betting line to the database
        for i, betting_line in enumerate(betting_lines):
            print(f'{((i + 1) / n_lines):.2%}: {betting_line}')

            try:
                insert_betting_line(db_connection, cursor, betting_line)
            except Exception as error:
                print(f'Error occurred: {error}')
                traceback.print_exc()
                failed_betting_lines[i] = [betting_line, error, traceback.format_exc()]

        # Save list of failed betting lines
        with open(failed_filename, 'wb') as fp2:
            pickle.dump(failed_betting_lines, fp2)


if __name__ == '__main__':
    scrape_betting_lines_rotowire('betting_lines.pkl')

    # Establish the database connection
    db_connection = psycopg2.connect(**connection_parameters)
    cursor = db_connection.cursor()

    # Scrape all the box scores
    add_betting_lines_to_database('betting_lines.pkl', db_connection, cursor, 'failed_betting_lines.pkl')

    # Close the cursor and connection
    cursor.close()
    db_connection.close()

    # with open('failed_betting_lines.pkl', 'rb') as fp:
    #     failed = pickle.load(fp)
    #     failed_keys = list(failed.keys())
    #     for key in failed_keys[len(failed_keys)-1:]:
    #         print(failed[key][1])
