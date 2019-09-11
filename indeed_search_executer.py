"""
The purpose of this module is to read what is in the search queue as expressed
in the table indeed_search set, and pass it off to the the indeed search executer.

It will pass out a Serch class to the search executer, and receive one back with
the search completed and search run date fields modified ; then this

"""

from sqlalchemy import Integer, String, Date, DateTime, Boolean, MetaData, Column, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# This is my own login module
from blunt_skull_tools import create_pg_login_string
from indeed_search_function import isf

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
    search_run_date = Column(DateTime)


# Start here  #
# create the engine
db_string = create_pg_login_string()
db_six_cyl_engine = create_engine(db_string, echo=False)
conexion = db_six_cyl_engine.connect()
# use SQL Alchemy to pull the metadata for the database down.
metadata = MetaData(db_six_cyl_engine)

# Declare the class to mirror the table structure.
Base = declarative_base()
Base.metadata.create_all(db_six_cyl_engine)

# Initiate a session based on the engine.
Session = sessionmaker()
session = Session(bind=db_six_cyl_engine)

# First thing is query the table for everything where search completed is true
yet_to_be_done_searches = session.query(Serch).filter_by(search_completed=False).order_by('iss_pk').all()

# Iterate through the uncompleted results and send each one to the search
for joolie in yet_to_be_done_searches:
    out_result = isf(joolie)
    joolie = out_result

# commit and quit!
session.commit()
session.close()
