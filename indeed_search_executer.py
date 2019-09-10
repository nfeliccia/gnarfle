"""
The purpose of this module is to read what is in the search queue as expressed
in the table indeed_search set, and pass it off to the the indeed search executer.

It will pass out a Serch class to the search executer, and receive one back with
the search completed and search run date fields modified ; then this

"""
import datetime

from sqlalchemy import Integer, String, Date, DateTime, Boolean, MetaData, Column, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# This is my own login module
from aws_login_credentials import awlc
from indeed_search_function_01 import isf

# This tells SQL Alchemny that I'm going to declare some jawns.
Base = declarative_base()


# This creates a class which mirrors the construction of the table indeed_search_set.
class Serch(Base):
    __tablename__ = 'indeed_search_set'
    iss_pk = Column(Integer, primary_key=True)
    search_keyword_list = Column(String)
    search_zip_code = Column(String)
    creation_date = Column(Date)
    search_completed = Column(Boolean)
    search_run_date = Column(Date)


def pseudo_search(in_serch_class):
    isf(in_serch_class)
    out_serch_class = in_serch_class
    search_result = input("did it work? [Y/N])").lower()
    if search_result == 'y':
        out_serch_class.search_completed = True
        out_serch_class.search_run_date = datetime.datetime.now()
    else:
        out_serch_class.search_completed = False
        out_serch_class.search_run_date = None
    return out_serch_class


### Start here  #

# Read the user and password file.
login_file = 'login_info.json'
# The log
# The login credentials tuple contains db_string, username, password, working_db
# the ask ok flag bypasses the asking if the login credentials are OK becuase
# continually entering y in development is a pain in the butt.
login_credentials_tuple = awlc(login_file, askok=False)
db_string = login_credentials_tuple[0]
user_id = login_credentials_tuple[1]
password = login_credentials_tuple[2]
working_db = login_credentials_tuple[3]
# create the string from the variables and create the engine
db_string = f'postgres+psycopg2://{user_id}:{password}@{db_string}:5432/{working_db}'
db_six_cyl_engine = create_engine(db_string, echo=False)
conexion = db_six_cyl_engine.connect()
metadata = MetaData(db_six_cyl_engine)

# Declare the class to mirror the table structure.
Base = declarative_base()


class Serch(Base):
    __tablename__ = 'indeed_search_set'
    iss_pk = Column(Integer, primary_key=True)
    search_keyword_list = Column(String)
    search_zip_code = Column(String)
    creation_date = Column(Date)
    search_completed = Column(Boolean)
    search_run_date = Column(DateTime)


Base.metadata.create_all(db_six_cyl_engine)

# Initiate a session based on the engine.
Session = sessionmaker()
session = Session(bind=db_six_cyl_engine)

# First thing is query the table for everything where search completed is true
yet_to_be_done_searches = session.query(Serch).filter_by(search_completed=False).order_by('iss_pk').all()

# Iterate through the uncompleted results and send each one to the search
for search in yet_to_be_done_searches[:5]:
    out_result = pseudo_search(search)
    search = out_result

# commit and quit!
session.commit()
session.close()
