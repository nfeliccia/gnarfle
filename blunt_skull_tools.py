import datetime
import re
import pandas as pd

from bs4 import NavigableString
from sqlalchemy import Integer, String, Date, DateTime, Boolean, Column, create_engine, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from aws_login_credentials import awlc

Base = declarative_base()


class Serch(Base):
    __tablename__ = 'indeed_search_set'
    creation_date = Column(Date)
    iss_pk = Column(Integer, primary_key=True)
    search_completed = Column(Boolean)
    search_keyword_list = Column(String)
    search_run_date = Column(DateTime)
    search_zip_code = Column(String)


class Rezult(Base):
    __tablename__ = 'indeed_search_results'
    company = Column(String)
    extracted_url = Column(String)
    guid = Column(String)
    isr_pk = Column(Integer, primary_key=True)
    iss_pk = Column(Integer)
    job_title_row = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    publish_date = Column(DateTime)
    scraped = Column(Boolean)

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

    def map_bs_to_class(self, in_search_item, isr_key):
        self.job_title_row = in_search_item[0].string
        self.extracted_url = Rezult.trim_indeed_url(in_search_item[2].string)
        self.company = in_search_item[4].string
        self.guid = in_search_item[5].string
        self.latitude = float(in_search_item[8].string.split(' ')[0])
        self.longitude = float(in_search_item[8].string.split(' ')[1])
        self.publish_date = datetime.datetime.strptime(in_search_item[6].string, f'%a, %d %b %Y %H:%M:%S GMT')
        self.scraped = False
        self.iss_pk = isr_key


class Text_Process(Base):
    __tablename__ = 'text_processing_queue'
    isr_pk = Column(Integer)
    tpq_pk = Column(Integer, primary_key=True)
    job_description = Column(String)
    jd_processed = Column(Boolean)


class Porpus_dictinary(Base):
    __tablename__ = "corpus_dictionary"
    cd_pk = Column(Integer, primary_key=True)
    phrase = Column(String)
    phrase_count = Column(Integer)
    isr_pk = Column(Integer)


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


def create_headers_for_the_browser():
    # headers for the Browser.
    # split up to keep on one line.
    user_agent_pt_1 = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    user_agent_pt_2 = '(KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
    return {'User-Agent': user_agent_pt_1 + user_agent_pt_2}


def start_a_sql_alchemy_session():
    """ This starts  a session using SQL Alchemy tools
    """
    # initiate the database_session
    db_string = create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    DatabaseSession = sessionmaker()
    return DatabaseSession(bind=db_six_cyl_engine)


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

def build_search_set_from_xls():
    db_string = create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    iss_template_filename = r'iss_template.xlsx'
    iss_template_df = pd.read_excel(iss_template_filename)
    iss_template_df = iss_template_df.astype({'search_keyword_list': str, 'search_zip_code': str})
    iss_template_df['search_run_date'] = pd.to_datetime(iss_template_df['search_run_date'],format='%m/%d/%Y')
    iss_template_df.to_sql('indeed_search_set',con=db_six_cyl_engine,if_exists='append',index=False)
