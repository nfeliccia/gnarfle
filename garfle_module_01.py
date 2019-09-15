import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text, create_engine

import gnarfle_module_bst as bst

# Set up one and only one BS session
program_start = time.time()

beautiful_soup_session = requests.Session()
session_with_remulak = bst.start_a_sql_alchemy_session()


def read_an_excel_sheet(in_excel_file, in_excel_sheet):
    """
    # the information to be searched is read off the indeed_search_set.xlsx. I use excel instead of
    # csv because excel keeps and passes datatype.
    in the future a more elegant interface can be built.
     There are multiple rows in this , so I set up an iterable to go through each of the rows.
    :param in_excel_file:
    :param in_excel_sheet
    :return: an iterrows dataframe object
    """
    return pd.read_excel(in_excel_file, in_excel_sheet).iterrows()


def query_indeed_and_grab_a_jobs_result_set(in_series, in_page_num):
    """
    This block creates the URL to send to indeed, uses Beautiful Soup to get the
    :param in_series: Pandas data series
    :param in_page_num: integer
    :return: beautiful Soup result set
    """
    indeed_url_part_1 = f"http://rss.indeed.com/rss?q={chr(34)}{in_series.search_keyword_list}{chr(34)}"
    indeed_url_part_2 = f'&l={in_series.search_zip_code}&start={str(in_page_num)}'
    search_results_page = f"{indeed_url_part_1}{indeed_url_part_2}"
    search_results_page_tree = beautiful_soup_session.get(search_results_page,
                                                          headers=bst.create_headers_for_the_browser())
    search_results_page_soup = BeautifulSoup(search_results_page_tree.content, 'lxml')
    return search_results_page_soup.find_all('item')


def create_job_description_soup(in_job_desc_url):
    """
    This takes in an indeed job descriptino URL and retursn a BS result set of the contents.
    :param in_job_desc_url:
    :return: a BS result set
    """
    detailed_job_description_page_tree = beautiful_soup_session.get(in_job_desc_url,
                                                                    headers=bst.create_headers_for_the_browser())
    return BeautifulSoup(detailed_job_description_page_tree.content, 'lxml')


def single_job_result_processing(in_single_job_result):
    # Create an instance and build the data in the ISJR Class with the inbuilt method called populate
    this_isjr_result = bst.IndeedJobSearchResult()
    this_isjr_result.populate(in_single_job_result)
    # Take the job URL and scrape it for the text.
    detailed_job_description_page_soup = create_job_description_soup(this_isjr_result.extracted_url)
    # Create an instance of the Indeed Job descripiton Analysis info.
    this_indeed_job_description_analysis = bst.IndeedJobDescriptionAnalysis()
    this_indeed_job_description_analysis.populate(detailed_job_description_page_soup)
    # creaet an incidence of the SQL Alchemy based class SQLIndeedSearchResults
    this_SQLUpload = bst.SQLIndeedSearchResults()
    this_SQLUpload.populate(this_isjr_result, this_indeed_job_description_analysis)
    session_with_remulak.add(this_SQLUpload)
    print(f"{this_isjr_result.guid} Done \t Currently Searching {indeed_search_row.search_zip_code}")


# Begin
excel_file = 'indeed_search_set.xlsx'
excel_sheet = 'indeed_search_set'

for isr_pk, indeed_search_row in read_an_excel_sheet(excel_file, excel_sheet):
    try:
        page_num = 0
        keep_searching = True
        while keep_searching:
            job_result_set = query_indeed_and_grab_a_jobs_result_set(indeed_search_row, page_num)
            # check to make sure positive results are still being received
            if len(job_result_set) < 8 or page_num > 20:
                keep_searching = False
            else:
                page_num += 10
            for single_job_result in job_result_set:
                single_job_result_processing(single_job_result)
    except AttributeError as oh_skite:
        print(f"Bad Read")

print(f'before commit  {(time.time() - program_start) * 1000} ')
session_with_remulak.commit()
beautiful_soup_session.close()

print(f'after  commit  {(time.time() - program_start) * 1000} ')

print(f'Dedup Started {(time.time() - program_start) * 1000}')
sql_pt_1 = 'WITH singled_out as (select distinct ON (guid) guid from indeed_search_results) '
sql_pt_2 = 'DELETE FROM indeed_search_results WHERE indeed_search_results.guid NOT IN '
sql_pt_3 = '(SELECT singled_out.guid FROM singled_out);  '
full_query = text(sql_pt_1 + sql_pt_2 + sql_pt_3)
session_with_remulak.execute(full_query)

print(f'Dedup ended {(time.time() - program_start) * 1000}')
session_with_remulak.close()