from flask import Flask
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import sqlite3
from threading import Thread
import yagmail
from config import email_config
from flask_apscheduler import APScheduler

#Create a config file in the same folder and put the configuration in this format:
'''
email_config = {
    'user': '',
    'password': ''
}
'''
yag = yagmail.SMTP(email_config['user'], email_config['password'])


def load_driver(binary_location='/opt/brave.com/brave/brave', path='./chromedriver-linux64/chromedriver'):
    option = webdriver.ChromeOptions()
    option.binary_location = binary_location
    s = Service(path)

    driver = webdriver.Chrome(service=s, options=option)
    driver.set_window_size(1366, 768)
    
    return driver


def get_link(driver, job = 'data-scientist', location = 'Morocco'):
    
    driver.get(f"https://www.linkedin.com/jobs/{job}-jobs?location={location}&f_TPR=r2592000")
    time.sleep(3)

    print(driver.current_url)

def scroll(driver, n_scrolls=10, afterEnd=5):
    for i in tqdm(range(n_scrolls)):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 100)")

        time.sleep(2)
        if n_scrolls <= i <= n_scrolls+afterEnd:
            try:
                scroll_button = driver.find_element(By.CLASS_NAME, 'infinite-scroller__show-more-button')
                scroll_button.click()
                time.sleep(3)
            except Exception:
                pass

def collect(driver):
    jobs = {
        'Title':[],
        'Description':[],
        'Company':[],
        'Location':[],
        'Actively_Hiring': [],
        'ListDate':[],
        'DaysAgo':[],
        'Link':[]
    }

    ul_element = driver.find_element(By.CLASS_NAME, "jobs-search__results-list")
    jobs_list = ul_element.find_elements(By.TAG_NAME, "li")
    l = len(jobs_list)
    print("Founded jobs:", l)

    for i in tqdm(range(l)):
        try:  
            element = jobs_list[i]
            element.click()
            time.sleep(1.3)

            title = element.find_element(By.CLASS_NAME,"base-search-card__title").text
            description = driver.find_element(By.CLASS_NAME,"show-more-less-html__markup").text
            link = element.find_element(By.TAG_NAME,"a").get_attribute('href')
            Company = element.find_element(By.CLASS_NAME,"base-search-card__subtitle").text
            Location = element.find_element(By.CLASS_NAME,"job-search-card__location").text
            try:
                ListDate = element.find_element(By.CLASS_NAME,"job-search-card__listdate").text
            except Exception:
                ListDate = ""
            try:
                Hiring = "Yes" if element.find_element(By.CLASS_NAME, "job-search-card__benefits").text else "No"
            except Exception:
                Hiring = "No"

            try:
                duration = ListDate.split(" ")[1]
                period = int(ListDate.split(" ")[0]) * (1 if duration in ['days','day'] else 7 if duration in ['week','weeks'] else 30 if duration in ['month','months'] else 0)
            except Exception:
                period = np.nan

            jobs['Title'].append(title)
            jobs['Description'].append(description)
            jobs['Link'].append(link)
            jobs['Company'].append(Company)
            jobs['Location'].append(Location)
            jobs['ListDate'].append(ListDate)
            jobs['Actively_Hiring'].append(Hiring)
            jobs['DaysAgo'].append(period)
            
        except Exception as e:
            print(f"An exception occurred: {e}")
            break

    return pd.DataFrame(jobs)

def data_to_csv(data):
    today = pd.to_datetime('today').strftime('%Y-%m-%d')
    df_ = data.sort_values('DaysAgo')
    df_['Latest'] = 1
    df_['Scraped'] = today
    df_.to_csv(f"jobs_{today}.csv")
    print(df_.head(10))

    return df_

def data_to_sql(data):
    con = sqlite3.connect("JobsData.db")
    cur = con.cursor()
    try:
        cur.execute("UPDATE your_table_name SET Latest = 0")
        print('All columns has been updated')
    except Exception as e:
        print('Something went wrong: ',e)

    data.to_sql("JOBS", cur, if_exists='append', index=False, method="multi")

    cur.execute("PRAGMA table_info('JOBS')")
    results = cur.fetchall()
    print("Table Information for 'JOBS':", results)
    con.commit()
    con.close()

scraping_done = False

def scrape_and_save_jobs():
    try:
        driver = load_driver()

        get_link(driver)
        scroll(driver,n_scrolls=11)
        jobs_data = collect(driver)
        print(jobs_data)

        sorted_jobs_data = data_to_csv(jobs_data)
        data_to_sql(sorted_jobs_data)
        
        global scraping_done
        scraping_done = True
        

        driver.quit()
        return "<p>Job data has been successfully scraped and saved!</p>"

    except Exception as e:
        print(f"An error occurred: {e}")
        try:
            driver.quit()
        except:
            pass
        return "<p>An error occurred while scraping jobs.</p>", 500

def mailing():
    today = pd.to_datetime('today').strftime('%Y-%m-%d')

    yag.send(
        to='jabran20029@gmail.com',
        subject='Job Data Scraping Results',
        contents='Please find the attached CSV file containing the scraped job data.',
        attachments=f"./jobs_2023-12-23.csv"
    )
    print("Mail has been sent Succefully")

#**************************************************#
from webdriver_manager import ChromeDriverManager

def load_driver_online():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver. Chrome (service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    return driver
#**************************************************#

app = Flask(__name__)

@app.route("/")
def index():
    global scraping_done

    if not scraping_done:
        thread = Thread(target=scrape_and_save_jobs)
        thread.start()

        return "<p>Job data scraping is in progress...</p>"
    else:
        return "<p>Job data has been scraped and saved.</p>"

scheduler = APScheduler()
if __name__ == '__main__':
    #scheduler.add_job(id='scrape_and_save_jobs', func=scrape_and_save_jobs, trigger='cron', day_of_week = 'mon-sun', hour=20, minute=30)
    scheduler.add_job(id='mailing', func=mailing, trigger='cron', day_of_week = 'mon-sun', hour=00, minute=16)
    scheduler.start()

    app.run(debug=True, use_reloader=False)