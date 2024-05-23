import time
import pickle
import requests
from bs4 import BeautifulSoup


def scrape_box_score_links_bbref(months, days, years):
    # Template URL for link pages
    bbref_url = 'https://www.basketball-reference.com/boxscores/?month={month}&day={day}&year={year}'

    box_score_links = []

    # Loop over every day in given range
    for year in years:
        for month in months:
            for day in days:
                print(f'{year}-{month:02}-{day:02}')

                # Load HTML for box score links on this day
                url = bbref_url.format(month=month, day=day, year=year)
                data = requests.get(url)

                # Find all links to box scores
                soup = BeautifulSoup(data.text, "html.parser")
                links = soup.find_all('a', string="Box Score")
                for link in links:
                    print(link['href'])
                    box_score_links.append(link['href'])
                print('\n\n\n')

                # Sleep for 2 seconds to ensure <= 30 requests per minute (required by bbref)
                time.sleep(2)

    # Save list of links to file for later use (and reuse)
    with open('bbref_box_score_links.pkl', 'wb') as fp:
        pickle.dump(box_score_links, fp)


if __name__ == '__main__':
    months = range(1, 13)
    days = range(1, 32)
    years = range(2000, 2025)
    scrape_box_score_links_bbref(months, days, years)
