import time
import pickle
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# service = Service('/Users/brandonmichaud/Projects/chromedriver')
# driver = webdriver.Chrome(service=service)
#
#
# def scrape_grouped_box_score_links(months, days, years):
#     nba_url = 'https://www.nba.com/games?date={year}-{month:02}-{day:02}'
#     bbref_url = 'https://www.basketball-reference.com/boxscores/?month={month}&day={day}&year={year}'
#     box_score_links = []
#     for year in years:
#         for month in months:
#             for day in days:
#                 print(f'{year}-{month:02}-{day:02}')
#                 url = nba_url.format(month=month, day=day, year=year)
#                 # data = requests.get(url)
#                 driver.get(url)
#                 WebDriverWait(driver, 10).until(
#                     lambda driver: driver.find_elements(By.XPATH, "//a[contains(text(), 'BOX SCORE')]") or
#                                    driver.find_elements(By.CLASS_NAME, "NoDataMessage_base__xUA61")
#                 )
#                 # elements = WebDriverWait(driver, 10).until(
#                 #     lambda driver: driver.find_elements(By.XPATH, "//a[contains(text(), 'BOX SCORE')]")
#                 #     if driver.find_elements(By.XPATH, "//a[contains(text(), 'BOX SCORE')]")
#                 #     else driver.find_element(By.CLASS_NAME, "NoDataMessage_base__xUA61")
#                 # )
#
#                 elements = driver.find_elements(By.XPATH, "//a[contains(text(), 'BOX SCORE')]")
#
#                 if elements:
#                     for element in elements:
#                         print(element.get_attribute('href'))
#                         box_score_links.append(element.get_attribute('href'))
#                 else:
#                     print('No Games')
#                 print('\n\n\n')
#                 # if elements.tag_name == "a" and "BOX SCORE" in elements.text:
#                 #     print("Box Score link found and visible.")
#                 #     print(elements.get_attribute('href'))
#                 #     # perform actions with the box score link
#                 # elif elements.tag_name == "div" and "nodata" in elements.get_attribute("class"):
#                 #     print("No data available.")
#
#                 # html = driver.page_source
#                 # with open('grouped_box_scores/{}{:02}{:02}.html'.format(year, month, day), 'w+', encoding="utf-8") as f:
#                 #     f.write(html)
#                 # soup = BeautifulSoup(html, "html.parser")
#                 # links = soup.find_all('a', string="BOX SCORE")
#                 # for link in links:
#                 #     print(link['href'])
#                 #     box_score_links.append(box_score_links)
#                 # print('\n\n\n')
#     driver.quit()
#     with open('nba_box_score_links.pkl', 'wb') as fp:
#         pickle.dump(box_score_links, fp)


def alt(months, days, years):
    bbref_url = 'https://www.basketball-reference.com/boxscores/?month={month}&day={day}&year={year}'
    box_score_links = []
    for year in years:
        for month in months:
            for day in days:
                print(f'{year}-{month:02}-{day:02}')
                url = bbref_url.format(month=month, day=day, year=year)
                data = requests.get(url)
                soup = BeautifulSoup(data.text, "html.parser")
                links = soup.find_all('a', string="Box Score")
                for link in links:
                    print(link['href'])
                    box_score_links.append(box_score_links)
                print('\n\n\n')
    with open('nba_box_score_links.pkl', 'wb') as fp:
        pickle.dump(box_score_links, fp)


if __name__ == '__main__':
    months = range(1, 13)
    days = range(1, 32)
    years = range(2000, 2025)
    # scrape_grouped_box_score_links(months, days, years)
    alt(months, days, years)
