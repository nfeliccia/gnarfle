"""
This module takes in a  class "Serch" from indeed search executer, and executes it!
"""
import datetime
import re

# For the Beautiful Soup stuff
import requests
from bs4 import BeautifulSoup
# For the SQL Achemy Stuff
from sqlalchemy import Integer, String, Date, DateTime, Boolean, Float, Column, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# from my own special collection
from aws_login_credentials import awlc

# declare the class for incoming base
Base = declarative_base()


class Serch(Base):
    __tablename__ = 'indeed_search_set'
    iss_pk = Column(Integer, primary_key=True)
    search_keyword_list = Column(String)
    search_zip_code = Column(String)
    creation_date = Column(Date)
    search_completed = Column(Boolean)
    search_run_date = Column(Date)


class Rezult(Base):
    __tablename__ = 'indeed_search_results'
    isr_pk = Column(Integer, primary_key=True)
    iss_pk = Column(Integer)
    guid = Column(String)
    publish_date = Column(DateTime)
    latitude = Column(Float)
    longitude = Column(Float)
    company = Column(String)
    extracted_url = Column(String)
    scraped = Column(Boolean)
    job_title_row = Column(String)

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

    def map_bs_to_class(self, in_search_item):
        self.job_title_row = in_search_item[0].string
        self.extracted_url = Rezult.trim_indeed_url(in_search_item[2].string)
        self.company = in_search_item[4].string
        self.guid = in_search_item[5].string
        self.latitude = float(in_search_item[8].string.split(' ')[0])
        self.longitude = float(in_search_item[8].string.split(' ')[1])
        self.publish_date = datetime.datetime.strptime(in_search_item[6].string, f'%a, %d %b %Y %H:%M:%S GMT')
        self.scraped = False
        self.iss_pk = test_dict['iss_pk']


def create_pg_login_string():
    login_file = 'login_info.json'
    login_credentials_tuple = awlc(login_file, askok=False)
    db_string = login_credentials_tuple[0]
    user_id = login_credentials_tuple[1]
    password = login_credentials_tuple[2]
    working_db = login_credentials_tuple[3]
    return f'postgres+psycopg2://{user_id}:{password}@{db_string}:5432/{working_db}'


# headers for the Browser.
# split up to keep on one line.
user_agent_pt_1 = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
user_agent_pt_2 = '(KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
headers = {'User-Agent': user_agent_pt_1 + user_agent_pt_2}


# This module will have a Serch class passed to it.
def isf(in_serch):
    # initiate the database_session I really gotta slim this down
    db_string = create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    DatabaseSession = sessionmaker()
    session_with_remulak = DatabaseSession(bind=db_six_cyl_engine)

    # This loop scans for result pages 10 at a time , just like indeed puts out.
    # Start at page 0
    page_num = 0
    keep_searching = True

    # create a session from the Requests module (not to be confused with a sql alechmy session)
    beautiful_soup_session = requests.Session()
    while keep_searching:
        # build the page as an f string from the search_Keyword List and search_zip_code
        page = f"http://rss.indeed.com/rss?q={chr(34)}{test_Serch.search_keyword_list}{chr(34)}&l={test_Serch.search_zip_code}&start=" + str(
            page_num)
        pageTree = beautiful_soup_session.get(page, headers=headers)
        pageSoup = BeautifulSoup(pageTree.content, 'html5lib')
        indeed_twenty_block = pageSoup.find_all('item')
        if len(indeed_twenty_block) == 20 and (page_num < 500):
            for job_listing in indeed_twenty_block:
                this_rezult = Rezult()
                this_rezult.map_bs_to_class(job_listing.contents)
                session_with_remulak.add(this_rezult)
                session_with_remulak.flush()
            page_num += 20
        else:
            keep_searching = False

    # clean up the duplicates
    sql_pt_1 = 'WITH singled_out as (select distinct ON (guid) guid,isr_pk from indeed_search_results) '
    sql_pt_2 = 'DELETE FROM indeed_search_results WHERE indeed_search_results.isr_pk NOT IN '
    sql_pt_3 = '(SELECT singled_out.isr_pk FROM singled_out);  '
    full_query = text(sql_pt_1 + sql_pt_2 + sql_pt_3)
    db_six_cyl_engine.execute(full_query)
    session_with_remulak.commit()
    session_with_remulak.close()


# This will be a test value we use while creating
test_dict = {'iss_pk': 8, 'search_keyword_list': 'account+executive', 'search_zip_code': '30303',
             'creation_date': datetime.date(2019, 9, 10), 'search_completed': False,
             'search_run_date': datetime.datetime.now()}

test_Serch = Serch(**test_dict)
print(test_Serch.search_keyword_list, test_Serch.search_zip_code)
isf(test_Serch)