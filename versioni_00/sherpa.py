import pandas as pd
from sqlalchemy import create_engine

import blunt_skull_tools as bst
from indeed_search_executer import indeed_search_executer
from job_description_reader import job_description_reader
from description_to_count_set import parse_description_to_words_and_count


def add_in_the_excel():
    db_string_aite = bst.create_pg_login_string()
    db_six_cyl_engine_aite = create_engine(db_string_aite, echo=False)
    conexion_aite = db_six_cyl_engine_aite.connect()
    iss_template_df = pd.read_excel('iss_template.xlsx')
    iss_template_df.to_sql('indeed_search_set', con=conexion_aite, if_exists='append', index=False)
    conexion_aite.close()


def check_the_search_set():
    session_with_remulak_css = bst.start_a_sql_alchemy_session()
    count_of_phrases_to_search_css = session_with_remulak_css.query(bst.Serch).filter_by(search_completed=False).count()
    print(f"Currently there are {count_of_phrases_to_search_css} searches in the keyword search queue ")
    if count_of_phrases_to_search_css == 0:
        print("All caught up on Searches")
    import_from_excel_sheet = input('Do you want to import the searches on the Excel Sheet?:[y/n]')
    if import_from_excel_sheet.lower() == 'y':
        print("You typed Y")
        add_in_the_excel()
        session_with_remulak_css.close()
        return True
    else:
        session_with_remulak_css.close()
        return True


def check_the_search_results():
    session_with_remulak_csr = bst.start_a_sql_alchemy_session()
    count_of_phrases_to_search_csr = session_with_remulak_csr.query(bst.Rezult).filter_by(scraped=False).count()
    print(f"Currently there are {count_of_phrases_to_search_csr} jobs to scrape ")
    if count_of_phrases_to_search_csr==0 :
        print("All caught up on Scraping")
        return False
    else:
        scrape_them_jobs = input('Do you want to scrape these?:[y/n]')
        if scrape_them_jobs.lower() == 'y':
            print("You typed Y")
            return True
        else:
            return False


def check_the_parsing():
    session_with_remulak_ctp = bst.start_a_sql_alchemy_session()
    count_of_phrases_to_search_ctp = session_with_remulak_ctp.query(bst.Text_Process).filter_by(jd_processed=False).count()
    print(f"Currently there are {count_of_phrases_to_search_ctp} descriptions to parse ")
    if count_of_phrases_to_search_ctp == 0:
        print("All caught up on parsing")
        session_with_remulak_ctp.close()
        return False
    else:
        scrape_them_jobs = input('Do you want to parse these?:[y/n]')
        if scrape_them_jobs.lower() == 'y':
            print("You typed Y")
            session_with_remulak_ctp.close()
            return True
        else:
            session_with_remulak_ctp.close()
            return False


if check_the_search_set():
    indeed_search_executer()
if check_the_search_results():
    job_description_reader()
if check_the_parsing():
    parse_description_to_words_and_count()