import re
from collections import namedtuple
from itertools import permutations

from sqlalchemy import Integer, String, Date, Float, Column
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from aws_login_credentials import awlc


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


def super_clean_a_string(in_string: str):
    out_string = re.sub(r'[,.;@%#?!&$()^*/\|’\':·\-]+\ *', " ", in_string).rstrip().lstrip().casefold()
    return out_string


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


session_with_remulak = start_a_sql_alchemy_session()

scramble_list_words = []
scramble_results_tuple_list = []
scramble_phrase_list = []
keep_getting_phrases = True
ScrambleResult = namedtuple('ScrambleResult', ['scramble_phrase', 'ocurances_in_title', 'ocurances_in_text'],
                            rename=True)

print("Type 'quit' to stop entering phrases")
while keep_getting_phrases:
    scramble_phrase = input("Get phrase to scramble:")
    scramble_phrase = super_clean_a_string(scramble_phrase)
    if scramble_phrase == 'quit':
        keep_getting_phrases = False
    else:
        scramble_split = scramble_phrase.split()
        for i in range(1, len(scramble_split) + 1):
            scramble_permutations = permutations(scramble_split[:i])
            scramble_phrase_list.extend([' '.join(group) for group in scramble_permutations])


