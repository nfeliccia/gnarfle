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

