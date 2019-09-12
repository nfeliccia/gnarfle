"""
This module takes in a  class "Serch" from indeed search executer, and executes it!
"""
import datetime

# For the Beautiful Soup stuff
import requests
from bs4 import BeautifulSoup
# For the SQL Achemy Stuff
from sqlalchemy import text

# from my own special collection
import blunt_skull_tools as bst


# This module will have a Serch class passed to it.
def isf(in_serch: bst.Serch):
    session_with_remulak = bst.start_a_sql_alchemy_session()
    # This loop scans for result pages 10 at a time , just like indeed puts out.
    # Start at page 0
    page_num = 0
    keep_searching = True
    headers = bst.create_headers_for_the_browser()
    # create a session from the Requests module (not to be confused with a sql alchemy session)
    beautiful_soup_session = requests.Session()
    print(f'Search Row{in_serch.iss_pk} being executed...')
    while keep_searching:
        # build the page as an f string from the search_Keyword List and search_zip_code
        page = f"http://rss.indeed.com/rss?q={chr(34)}{in_serch.search_keyword_list}{chr(34)}&l={in_serch.search_zip_code}&start=" + str(
            page_num)
        page_tree = beautiful_soup_session.get(page, headers=headers)
        page_soup = BeautifulSoup(page_tree.content, 'html5lib')
        indeed_twenty_block = page_soup.find_all('item')
        if len(indeed_twenty_block) == 20 and (page_num < 50):
            for job_listing in indeed_twenty_block:
                this_rezult = bst.Rezult()
                this_rezult.map_bs_to_class(job_listing.contents, in_serch.iss_pk)
                session_with_remulak.add(this_rezult)
                session_with_remulak.flush()
            page_num += 20
        else:
            keep_searching = False

    # commit and quit!
    session_with_remulak.commit()
    session_with_remulak.close()
    in_serch.search_completed = True
    in_serch.search_run_date = datetime.datetime.now()

    second_session_with_remulak = bst.start_a_sql_alchemy_session()
    # clean up the duplicates
    sql_pt_1 = 'WITH singled_out as (select distinct ON (guid) guid,isr_pk from indeed_search_results) '
    sql_pt_2 = 'DELETE FROM indeed_search_results WHERE indeed_search_results.isr_pk NOT IN '
    sql_pt_3 = '(SELECT singled_out.isr_pk FROM singled_out);  '
    full_query = text(sql_pt_1 + sql_pt_2 + sql_pt_3)
    second_session_with_remulak.execute(full_query)
    second_session_with_remulak.commit()
    second_session_with_remulak.close()

    return in_serch
