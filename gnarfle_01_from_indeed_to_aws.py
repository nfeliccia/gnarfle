import time
from datetime import datetime as dt2

import requests
from bs4 import BeautifulSoup

import gnarfle_00_bst_tools as bst


def build_on_the_indeed_search_list():
    """
    this function adds to the table indeed_search_queue in the remulak database
    It asks the users for phrases up to three words, then how many of the to 20 metro area
    zip codes they want to search, then on a double loop builds the list of zip codes in the search
    results.
    :return:
    """

    def how_deep_to_search_in_geogaphic_list():
        """
        In the zip_codes table, there are a list of the downtown zipcodes of the top 20+ metro areas.
        This allows the user to enter how many of the top 20 they want to search
        :return: int
        """
        global session_with_remulak
        # This block sets the depth of how many zip codes to search in the zip code list
        search_depth_input = input("How Deep on Geographic Search to go [enter is default of 20]:")
        # trap a blank or an alpha character
        if search_depth_input == "" or not search_depth_input.isnumeric():
            search_result_hdtsigl = 20
        else:
            # ensure that if someone enters a negative number it won't screw it up.
            # convert search_depth to a float
            search_result_hdtsigl = max(1, float(search_depth_input))
        return search_result_hdtsigl

    search_word_bank = bst.get_big_set_of_search_words_for_indeed()
    search_depth = how_deep_to_search_in_geogaphic_list()

    # get the list of zip_codes - return a class.
    zip_code_query = session_with_remulak.query(bst.ZipCodes).filter(bst.ZipCodes.metro_area_rank < search_depth)
    zip_code_list=zip_code_query.all()
    for set_of_search_words in search_word_bank:
        for zip_code_class in zip_code_list:
            this_indeed_search_row = bst.IndeedSearchQueue()
            this_indeed_search_row.search_keyword_list = set_of_search_words
            this_indeed_search_row.creation_date = dt2.now()
            this_indeed_search_row.search_completed = False
            this_indeed_search_row.search_zip_code = zip_code_class.zip_code
            session_with_remulak.add(this_indeed_search_row)

    # print(zip_codes)
    print("Beginning Commit")
    session_with_remulak.commit()
    print("Done Commit")


def move_single_job_result_from_indeed_to_database(in_single_job_result):
    """

    :param in_single_job_result:
    :return:
    """

    def create_job_description_soup(in_job_desc_url: str):
        """
        This takes in an indeed job descriptino URL and retursn a BS result set of the contents.
        :param in_job_desc_url:string
        :return: a BS result set
        """
        detailed_job_description_page_tree = beautiful_soup_session.get(in_job_desc_url,
                                                                        headers=bst.create_headers_for_the_browser())
        return BeautifulSoup(detailed_job_description_page_tree.content, 'lxml')

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


def query_indeed_and_grab_a_jobs_result_set(in_isq, in_page_num):
    """
    This block creates the URL to send to indeed, uses Beautiful Soup to get the
    :param in_isq: Pandas data series
    :param in_page_num: integer
    :return: beautiful Soup result set
    """
    indeed_url_part_1 = f"http://rss.indeed.com/rss?q={chr(34)}{in_isq.search_keyword_list}{chr(34)}"
    indeed_url_part_2 = f'&l={in_isq.search_zip_code}&start={str(in_page_num)}'
    search_results_page = f"{indeed_url_part_1}{indeed_url_part_2}"
    print(f'Searching {search_results_page}')
    search_results_page_tree = beautiful_soup_session.get(search_results_page,
                                                          headers=bst.create_headers_for_the_browser())
    search_results_page_soup = BeautifulSoup(search_results_page_tree.content, 'lxml')
    return search_results_page_soup.find_all('item')


# Set up one and only one BS session
program_start = time.time()
beautiful_soup_session = requests.Session()
session_with_remulak = bst.start_a_sql_alchemy_session()

work_the_search_list_user_input = input("Do you want to add to the current search list: ")
work_the_search_list_user_input_clean = bst.super_clean_a_string(work_the_search_list_user_input)
if work_the_search_list_user_input_clean == 'y':
    build_on_the_indeed_search_list()

# use SQL Alchemy to find all the pending jawns
isq_search_set_query = session_with_remulak.query(bst.IndeedSearchQueue).filter(
    bst.IndeedSearchQueue.search_completed == False).order_by(bst.IndeedSearchQueue.creation_date)

isq_search_set = isq_search_set_query.all()

for isq_search in isq_search_set:
    page_num = 0
    keep_searching = True
    try:
        while keep_searching:
            job_result_set = query_indeed_and_grab_a_jobs_result_set(isq_search, page_num)
            # check to make sure positive results are still being received
            if len(job_result_set) < 2 or page_num > 100:
                keep_searching = False
            else:
                page_num += 10
            for single_job_result in job_result_set:
                move_single_job_result_from_indeed_to_database(single_job_result)
    except AttributeError as oh_skyte_1:
        print("Bad Read")
    isq_search.search_completed = True
    isq_search.search_run_date = dt2.now()

print(f'before commit  {(time.time() - program_start)} ')
session_with_remulak.commit()
beautiful_soup_session.close()
print(f'after  commit  {(time.time() - program_start)} ')

session_with_remulak.close()
