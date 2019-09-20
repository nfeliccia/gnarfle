"""
This file holds all of the classes and common functions

"""
import datetime
import re
from datetime import datetime

from bs4 import NavigableString
from sqlalchemy import Integer, String, Date, Float, Column, Boolean, create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from aws_login_credentials import awlc

Base = declarative_base()


class IndeedSearchQueue(Base):
    __tablename__ = 'indeed_search_queue'
    isq_pk = Column(Integer, primary_key=True)
    search_keyword_list = Column(String)
    creation_date = Column(Date)
    search_completed = Column(Boolean)
    search_run_date = Column(Date)
    search_zip_code = Column(String)

    def __init__(self):
        self.isq_pk = None
        self.search_keyword_list = ""
        self.creation_date = None
        self.search_run_date = None
        self.search_zip_code = None


class IndeedJobDescriptionAnalysis:
    def __init__(self):
        self.job_title = "unknown"
        self.job_text_raw = ""

    def populate(self, in_soup):
        self.job_title = in_soup.h3.text
        self.job_text_raw = in_soup.find('div', {'id': 'jobDescriptionText'}).get_text()


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

    def populate(self, in_result, in_analysis):
        self.company = in_result.company
        self.extracted_url = in_result.extracted_url
        self.guid = in_result.guid
        self.job_title_row = in_result.job_title_row
        self.latitude = in_result.latitude
        self.longitude = in_result.longitude
        self.publish_date = in_result.publish_date
        self.job_text_raw = in_analysis.job_text_raw
        self.job_title = in_analysis.job_title

class Whack_Words(Base):
    __tablename__ = 'ref_whack_words'
    ww_pk = Column(Integer, primary_key=True)
    whack_word = Column(String)

class ZipCodes(Base):
    __tablename__ = 'ref_zip_codes'
    zipcodes_pk = Column(Integer, primary_key=True)
    zip_code = Column(String)
    metro_area_name = Column(String)
    metro_area_rank = Column(Float)


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


def get_big_set_of_search_words_for_indeed() -> list:
    """
    This loop gives us a list of search words to send to indeed_search_queue
    :return: list of strings
    """

    def get_single_set_of_search_words_for_indeed():
        """
        This function asks the user for search terms, whacks any strange characters, and then
        splits up the first three words. Any extra ones are ignored
        :return:A string of Keywords for an indeed search separated by a plus character
        """
        search_keywords = input("Enter Search Keyword List up to 3 words no punctuation\ntype 'quit' when done:")
        # I made a module called super clean a string which uses RE to get out special characters
        search_keywords = super_clean_a_string(search_keywords)
        # Split on spaces in case a special character was replaced with a space
        search_keyword_list = search_keywords.split(' ')
        # I rejoin with a plus because that's the search syntax for indeed.
        # I limit the first few elements of the list
        search_keywords_for_indeed = '+'.join(search_keyword_list[0:3])
        return search_keywords_for_indeed

    keep_getting_search_words = True
    search_word_bank_gbsoswfi = []
    while keep_getting_search_words:
        this_set_of_search_words = get_single_set_of_search_words_for_indeed()
        if this_set_of_search_words.find('quit') > -1:
            keep_getting_search_words = False
        else:
            search_word_bank_gbsoswfi.append(this_set_of_search_words)
    return search_word_bank_gbsoswfi


def start_a_sql_alchemy_session():
    """ This starts  a session using SQL Alchemy tools
    """
    # initiate the database_session
    db_string = create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    DatabaseSession = sessionmaker()
    return DatabaseSession(bind=db_six_cyl_engine)


def super_clean_a_string(in_string: str):
    out_string = re.sub(r'[,.;@%#?!&$()^*/’\':·\-]+\ *', " ", in_string).rstrip().lstrip().casefold()
    return out_string


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


def dedup_indeed_search_results(in_session: Session):
    """
    This is a routine which removes duplicates from the indeed search results. I put it here so
    it can be called anywhere.
    :param in_session:
    :return: None
    """
    sql_pt_1 = 'DELETE FROM indeed_search_results AS a USING (SELECT MIN(isr_pk) as isr_pk, guid FROM indeed_search_results '
    sql_pt_2 = 'GROUP BY guid HAVING COUNT(*) > 1 ) AS b WHERE a.guid = b.guid'
    sql_pt_3 = ' AND a.isr_pk <> b.isr_pk;  '
    full_query = text(sql_pt_1 + sql_pt_2 + sql_pt_3)
    in_session.execute(full_query)
    in_session.commit()
