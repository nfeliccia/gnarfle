"""
This function reads through all unscraped job descriptions and returns a list of phrases from the jd text
"""
# For the Beautiful Soup stuff

import requests
from bs4 import BeautifulSoup
# For the SQL Alchemy Stuff
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Project wide tools
import blunt_skull_tools as bst


def job_description_reader():
    # Starte.
    db_string = bst.create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    Base = declarative_base()
    Base.metadata.create_all(db_six_cyl_engine)
    DatabaseSession = sessionmaker()
    session_with_remulak  = DatabaseSession(bind=db_six_cyl_engine)
    # Prime that Beautiful Soup
    headers = bst.create_headers_for_the_browser()
    # create a session from the Requests module (not to be confused with a sql alchemy session)
    beautiful_soup_session = requests.Session()
    yet_to_be_scraped_group = session_with_remulak.query(bst.Rezult).filter_by(scraped=False).order_by('isr_pk')
    for yet_to_be_scraped in yet_to_be_scraped_group:
        page_tree = beautiful_soup_session.get(yet_to_be_scraped.extracted_url, headers=headers)
        print(f'now serving {yet_to_be_scraped.isr_pk}')
        single_job_soup = BeautifulSoup(page_tree.content, 'lxml')
        this_desc_out = bst.Text_Process()
        # get job title
        try:
            job_title = single_job_soup.h3.text
        except AttributeError as oh_skyte_1:
            print(oh_skyte_1)
            job_title = "unknown"
        job_text = single_job_soup.find('div', {'id': 'jobDescriptionText'})
        seed_text = ""
        try:
            for item in job_text.contents:
                description_snippet = bst.bs_showit(item, print_it_out=False)
                if len(description_snippet) > 0:
                    seed_text = seed_text + description_snippet + '\n'
                else:
                    seed_text = seed_text + '\n'
        except AttributeError as oh_skyte_2:
            print(oh_skyte_2)
            seed_text='unknown'

        # assign the changes to the class
        yet_to_be_scraped.scraped = True
        yet_to_be_scraped.job_title_row = job_title
        this_desc_out.isr_pk = yet_to_be_scraped.isr_pk
        this_desc_out.job_description = seed_text
        session_with_remulak.add(this_desc_out)
        session_with_remulak.flush()

    session_with_remulak.commit()
    session_with_remulak.close()


job_description_reader()
