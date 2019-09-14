import datetime
import re
from datetime import datetime

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

    def populate(self,in_result,in_analysis):
        self.company = in_result.company
        self.extracted_url = in_result.extracted_url
        self.guid = in_result.guid
        self.job_title_row = in_result.job_title_row
        self.latitude = in_result.latitude
        self.longitude = in_result.longitude
        self.publish_date = in_result.publish_date
        self.job_text_raw = in_analysis.job_text_raw
        self.job_title = in_analysis.job_title


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

    def populate(self, in_job_result):
        self.job_title_row = in_job_result.title.text
        self.extracted_url = trim_indeed_url(in_job_result.contents[2])
        self.company = in_job_result.contents[3].text
        self.publish_date = datetime.strptime(in_job_result.contents[5].string, f'%a, %d %b %Y %H:%M:%S GMT')
        self.guid = in_job_result.contents[4].text
        self.latitude = float(in_job_result.contents[7].string.split(' ')[0])
        self.longitude = float(in_job_result.contents[7].string.split(' ')[1])


class IndeedJobDescriptionAnalysis:
    def __init__(self):
        self.job_title = "unknown"
        self.job_text_raw = ""

    def populate(self, in_soup):
        self.job_title = in_soup.h3.text
        self.job_text_raw = in_soup.find('div', {'id': 'jobDescriptionText'}).get_text()


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


"""
attic
 # print(f'indeed search row {isr_pk} loop started at {(time.time()-program_start)*1000}' )
                 #     print(f" Guid:{this_isjr_result.guid}\n", f"Job Title Row {this_isjr_result.job_title_row}\n",
                #           f"Extracted URL {this_isjr_result.extracted_url}\n", f"Company {this_isjr_result.company}\n",
                #           f"Latitude {this_isjr_result.latitude}", f"Longitude {this_isjr_result.longitude}\n",
                #           f"Date {this_isjr_result.publish_date}", f"Job Title  {this_indeed_job_description_analysis.job_title}\n",
                #           f"{job_text_raw} ")
"""