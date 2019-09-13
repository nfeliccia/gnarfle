import datetime
import re
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString
from sqlalchemy import Integer, String, Date, Float, Column, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from aws_login_credentials import awlc

Base = declarative_base()


class SQLIndeedSearchResults(Base):
    __tablename__ = 'indeed_search_results'
    isr_pk = Column(Integer, primary_key=True)
    company = Column(String)
    extracted_url = Column(String)
    guid = Column(String)
    job_title_row = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    publish_date = Column(Date)
    job_text_raw = Column(String)
    job_title = Column(String)


class IndeedJobSearchResult:
    def __init__(self):
        self.job_title_row = ""
        self.extracted_url = ""
        self.company = ""
        self.guid = ""
        self.latitude = 0.0
        self.longitude = 0.0
        self.publish_date = datetime.now()
        self.scraped = False
        self.iss_pk = 0


class IndeedJobDescriptionAnalysis:
    def __init__(self):
        self.job_title = "unknown"
        self.guid = ""
        self.frequency_dictionary = {}


def bs_showit(in_bsthing, print_it_out=False):
    if print_it_out:
        if isinstance(in_bsthing, NavigableString):
            print(in_bsthing.string)
        else:
            print(in_bsthing.text)
    else:
        if isinstance(in_bsthing, NavigableString):
            return in_bsthing.string
        else:
            return in_bsthing.text


def create_headers_for_the_browser():
    # headers for the Browser.
    # split up to keep on one line.
    user_agent_pt_1 = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    user_agent_pt_2 = '(KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
    return {'User-Agent': user_agent_pt_1 + user_agent_pt_2}


def create_pg_login_string():
    """
    This function takes no input, reads the login file, parses the information and
    concatenates it into a string for the postress engine to use.
    :return: string
    """
    # The login credentials tuple contains db_string, username, password, working_db
    # the ask ok flag bypasses the asking if the login credentials are OK becuase
    # continually entering y in development is a pain in the butt.
    login_file = 'login_info.json'
    login_credentials_tuple = awlc(login_file, askok=False)
    aws_db_url = login_credentials_tuple[0]
    user_id = login_credentials_tuple[1]
    password = login_credentials_tuple[2]
    working_db = login_credentials_tuple[3]
    return f'postgres+psycopg2://{user_id}:{password}@{aws_db_url}:5432/{working_db}'


def start_a_sql_alchemy_session():
    """ This starts  a session using SQL Alchemy tools
    """
    # initiate the database_session
    db_string = create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    DatabaseSession = sessionmaker()
    return DatabaseSession(bind=db_six_cyl_engine)


def trim_indeed_url(self):
    """
    When a URL is reutrned in the XML, it has a lot of extra characters in it.   There are several reasons we want to
    simplify the URL. 1) Remove any identifying information  2) Ease of Storage  Note, I do leave in the from=rss
    text because If Indeed is counting usage of the RSS, I don't want it to go away.
    :param self: text
    :return: out_url:text
    """
    jk_snippet = re.search('jk=', self).start()
    rtk_snippet = re.search('&rtk=', self).start()
    front_snippet = self[0:30]
    out_url = f'{front_snippet}{self[jk_snippet:rtk_snippet]}&from=rss'
    return out_url


# Set up one and only one BS session
program_start = time.time()

beautiful_soup_session = requests.Session()
session_with_remulak = start_a_sql_alchemy_session()
# print(f'Soup session_created at {(time.time() - program_start) * 1000}')

# the information to be searched is read off the indeed_search_set.xlsx. I use excel instead of
# csv because excel keeps and passes datatype.
indeed_search_set_xls_df = pd.read_excel('indeed_search_set.xlsx', 'indeed_search_set')
# There are multiple rows in this , so I set up an iterable to go through each of the rows.
indeed_search_set_xls_df_iterable = indeed_search_set_xls_df.iterrows()
# print(f'excel read   at {(time.time() - program_start) * 1000} ')
# This loop goes through each search query -
for isr_pk, indeed_search_row in indeed_search_set_xls_df_iterable:
    try:
        page_num = 0
        # print(f'indeed search row {isr_pk} loop started at {(time.time()-program_start)*1000}' )
        indeed_url_part_1 = f"http://rss.indeed.com/rss?q={chr(34)}{indeed_search_row.search_keyword_list}{chr(34)}"
        indeed_url_part_2 = f'&l={indeed_search_row.search_zip_code}&start={str(page_num)}'
        search_results_page = f"{indeed_url_part_1}{indeed_url_part_2}"
        # print(f'url get command sent at {(time.time()-program_start)*1000} ')
        search_results_page_tree = beautiful_soup_session.get(search_results_page, headers=create_headers_for_the_browser())
        # print(f'url get command completed and page souping started at {(time.time()-program_start)*1000} ')
        search_results_page_soup = BeautifulSoup(search_results_page_tree.content, 'lxml')
        # print(f'Souping finsihed at {(time.time()-program_start)*1000}')
        # print(f'Find item started at  {(time.time() - program_start) * 1000}')
        single_job_result = search_results_page_soup.find('item')
        # print(f'Find item finished  at  {(time.time() - program_start) * 1000}')
        # print(f'Loading the this_IJSR Result Started at {(time.time() - program_start) * 1000} ')
        this_isjr_result = IndeedJobSearchResult()
        this_isjr_result.job_title_row = single_job_result.title.text
        this_isjr_result.extracted_url = trim_indeed_url(single_job_result.contents[2])
        this_isjr_result.company = single_job_result.contents[3].text
        this_isjr_result.publish_date = datetime.strptime(single_job_result.contents[5].string,
                                                          f'%a, %d %b %Y %H:%M:%S GMT')
        this_isjr_result.guid = single_job_result.contents[4].text
        this_isjr_result.latitude = float(single_job_result.contents[7].string.split(' ')[0])
        this_isjr_result.longitude = float(single_job_result.contents[7].string.split(' ')[1])
        # print(f'Loading the this_IJSR Result finshed  at {(time.time() - program_start) * 1000} ')
        # print(f'Loading the full job description data started  at {(time.time() - program_start) * 1000} ')
        detailed_job_description_page_tree = beautiful_soup_session.get(this_isjr_result.extracted_url,
                                                                        headers=create_headers_for_the_browser())
        # print(f'Loading the full job description data finished  at {(time.time() - program_start) * 1000} ')
        # print(f'Starting to soup parse the full job description  at {(time.time() - program_start) * 1000} ')
        detailed_job_description_page_soup = BeautifulSoup(detailed_job_description_page_tree.content, 'lxml')
        # print(f'Finished soup parse the full job description  at {(time.time() - program_start) * 1000} ')
        this_indeed_job_description_analysis = IndeedJobDescriptionAnalysis()
        this_indeed_job_description_analysis.job_title = detailed_job_description_page_soup.h3.text
        this_indeed_job_description_analysis.guid = this_isjr_result.guid
        # print(f'Started search for job description textat {(time.time() - program_start) * 1000} ')
        job_text_raw = detailed_job_description_page_soup.find('div', {'id': 'jobDescriptionText'}).get_text()
        #     print(f" Guid:{this_isjr_result.guid}\n", f"Job Title Row {this_isjr_result.job_title_row}\n",
        #           f"Extracted URL {this_isjr_result.extracted_url}\n", f"Company {this_isjr_result.company}\n",
        #           f"Latitude {this_isjr_result.latitude}", f"Longitude {this_isjr_result.longitude}\n",
        #           f"Date {this_isjr_result.publish_date}", f"Job Title  {this_indeed_job_description_analysis.job_title}\n",
        #           f"{job_text_raw} ")

        this_SQLUpload = SQLIndeedSearchResults()
        this_SQLUpload.company = this_isjr_result.company
        this_SQLUpload.extracted_url = this_isjr_result.extracted_url
        this_SQLUpload.guid = this_isjr_result.guid
        this_SQLUpload.job_title_row = this_isjr_result.job_title_row
        this_SQLUpload.latitude = this_isjr_result.latitude
        this_SQLUpload.longitude = this_isjr_result.longitude
        this_SQLUpload.publish_date = this_isjr_result.publish_date
        this_SQLUpload.job_text_raw = job_text_raw
        this_SQLUpload.job_title = this_indeed_job_description_analysis.job_title
        session_with_remulak.add(this_SQLUpload)
        print(f"{isr_pk} Done")
    except AttributeError as oh_skite:
        print(f"{oh_skite} {isr_pk} is bad")

print(f'ended at  {(time.time() - program_start) * 1000} ')
session_with_remulak.commit()
beautiful_soup_session.close()
session_with_remulak.close()
