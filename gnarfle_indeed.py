import time
from collections import Counter
from datetime import datetime as dt2
from random import randrange

import pandas as pd
import requests
from bs4 import BeautifulSoup
from nltk import PorterStemmer
from nltk import ngrams, word_tokenize
from nltk.corpus import stopwords
from sqlalchemy.orm.query import Query

import gnarfle_00_bst_tools as bst
from gnarfle_00_bst_tools import SQLIndeedSearchResults as SiSr


def description_word_frequency_aggregator(in_result: Query):
    """
    This function takes in the results for a query and aggregates the single description counter.
    :param in_result: Query
    :return: pandas data frame
    """
    counter_results_sum = Counter()
    print(f'\t\tStart building counter results list')
    counter_results_list = [word_frequency_for_a_single_description(in_result.job_text_raw) for in_result in
                            job_title_result_set]
    print(f'\t\tfinished building counter results list')
    print(f'\t\tBeginning summation of counter results ')
    # I use the .update command because its supposed to be the fastest to sum up counters.
    for counter_result in counter_results_list:
        counter_results_sum.update(counter_result)
    print(f'\t\tEnding summation of counter results')
    print(f'\t\tBeginning stopwords scrub')
    counter_results_scrubbed = stopwords_scrub(counter_results_sum)
    print(f'\t\tEnding stopwords scrub')
    result_to_present = counter_results_scrubbed.most_common(1000)
    print(f'\t\tBeginning conversion to data frame')
    word_count_df = pd.DataFrame(result_to_present, columns=['keyword', 'number_of_hits'])
    print(f'\t\tEnd conversion to data frame')
    return word_count_df


def get_the_length(row):
    return len(row['keyword'])


def make_a_comparison_file_name(in_keyword_counter_list: list):
    print(f'Begin writing comparison')
    # set a seed for the first name.
    comparison_filename_macfn = 'comparison'
    for info_tuple in in_keyword_counter_list:
        comparison_filename_macfn = comparison_filename_macfn + info_tuple[0] + '_'
    # needs to be tagged with appropriate filetime
    comparison_filename_macfn = comparison_filename_macfn + '.xlsx'
    # I put this in here to avoid the filename going over 255 and screwing up things further down the line
    comparison_filename_macfn = comparison_filename_macfn[:255]
    return comparison_filename_macfn


def stopwords_scrub(in_counter: Counter):
    # I start with the english stopwords
    words_to_whack = stopwords.words('english')
    # Query from the database the current stopword list. I'm keeping the stopword list on the db for easy uprade
    whack_words_query = swr.query(bst.Whack_Words.whack_word)
    # I combined the .all with the query and the list iterator because the query comes back as a list of tuples.
    whack_words_list = [word_tuple[0] for word_tuple in whack_words_query.all()]
    words_to_whack.extend(whack_words_list)
    # I sort so the shortest words and presumably the most frequent will be at the beginning of the list and caught first.
    words_to_whack.sort(key=len)
    out_counter = in_counter.copy()
    for element in out_counter.copy():
        if element in words_to_whack:
            del out_counter[element]
    return out_counter


def build_on_the_indeed_search_list():
    """
    this function adds to the table indeed_search_queue in the remulak database
    It asks the users for phrases up to three words, then how many of the to 20 metro area
    zip codes they want to search, then on a double loop builds the list of zip codes in the search
    results.
    :return:
    """

    def how_deep_to_search_in_geographic_list():
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
    search_depth = how_deep_to_search_in_geographic_list()

    # get the list of zip_codes - return a class.
    zip_code_query = session_with_remulak.query(bst.ZipCodes).filter(bst.ZipCodes.metro_area_rank < search_depth)
    zip_code_list = zip_code_query.all()
    for set_of_search_words in search_word_bank:
        for zip_code_class in zip_code_list:
            this_indeed_search_row = bst.IndeedSearchQueue()
            this_indeed_search_row.search_keyword_list = set_of_search_words
            this_indeed_search_row.creation_date = dt2.now()
            this_indeed_search_row.search_completed = False
            this_indeed_search_row.search_zip_code = zip_code_class.zip_code
            session_with_remulak.add(this_indeed_search_row)

    # print(zip_codes)
    print("Writing to search queue...")
    session_with_remulak.commit()
    print("Done search queue update")
    return search_word_bank


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
    search_results_page_tree = beautiful_soup_session.get(search_results_page,
                                                          headers=bst.create_headers_for_the_browser())
    search_results_page_soup = BeautifulSoup(search_results_page_tree.content, 'lxml')
    return search_results_page_soup.find_all('item')


def word_frequency_for_a_single_description(in_job_text: str):
    """
    The purpose of this function is to take in a raw string of job text scraped from the web,
    and get a frequency word count. I chose to return a counter object because they can be summed together.

    :param in_job_text: which is a string  of job text scraped from a job description
    :return: a Counter Object with teh word counts
    """
    # remove all sorts of punctuation to optimize word parsing.
    job_text_string = bst.super_clean_a_string(in_job_text)
    # tokenize for counting
    job_text_words = word_tokenize(job_text_string)
    # Turn everything to lower case. Using Casefold as it handels UTF-8 better.
    job_text_words = [word.casefold() for word in job_text_words]
    # create a list of the stems
    job_text_stems = [our_stemmer.stem(word) for word in job_text_words]
    job_text_multiples_list = []
    # I want to create phrases of two to three words
    for phrase_length in range(2, 5):
        job_text_multiples = ngrams(job_text_words, phrase_length)
        for multiple in job_text_multiples:
            # turn the multiple from a tuple into a phrase.
            additional_phrase = ' '.join(multiple)
            job_text_multiples_list.append(additional_phrase)
    # add the multiples list on to the main job words. Using one list to save memory.
    job_text_words.extend(job_text_multiples_list)
    job_text_words.extend(job_text_stems)
    job_desc_count = Counter(job_text_words)
    job_desc_count_scrubbed = stopwords_scrub(job_desc_count)
    return job_desc_count_scrubbed


# ---- BEGINNING of Program
# Set up one and only one BS session
program_start = time.time()
beautiful_soup_session = requests.Session()
session_with_remulak = bst.start_a_sql_alchemy_session()
keyword_counter_list = []
this_session_words = []

work_the_search_list_user_input = input("Do you want to add to the current search list: ")
work_the_search_list_user_input_clean = bst.super_clean_a_string(work_the_search_list_user_input)
if work_the_search_list_user_input_clean == 'y':
    this_session_words = build_on_the_indeed_search_list()

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
    print(f"Completed {isq_search.search_keyword_list} {isq_search.search_zip_code}")
    isq_search.search_run_date = dt2.now()

print(f'before commit ')
session_with_remulak.commit()
beautiful_soup_session.close()
print(f'after  commit ')

# I want to have stems available in the result counts.
our_stemmer = PorterStemmer()
# load in the stop words
stop_words = set(stopwords.words('english'))

# I want to keep the understanding that we're having  a session with the remote database remulac
# but want to alias it since it makes the statements long.
swr = session_with_remulak
# before you pull results , make sure that the indeed search results are d-duped.
bst.dedup_indeed_search_results(swr)
# I set the filename up here so I can do multiple writes without having it change. 

if not this_session_words:
    search_set = bst.get_big_set_of_search_words_for_indeed()
else:
    search_set = this_session_words.copy()

for search_phrase in search_set:
    # I had to put this in because too short of a search phrase returns too many darn results
    search_words = f'%{search_phrase.replace("+", " ")}%'
    if len(search_words) < 3:
        break
    # this is a quick and dirty way of handling less than three elements
    print(f'Testing {search_words}')
    print(f'\tFiltering {search_words} ')
    # I want to use SQL Alchemy - note the ilike command here makes it case insensitive.
    job_title_result_query = swr.query(SiSr.job_title, SiSr.job_text_raw).filter(
        SiSr.job_title.ilike(search_words)).order_by(SiSr.publish_date.desc())
    job_title_result_set = job_title_result_query.all()
    job_title_result_count = job_title_result_query.count()
    # I need to do the [1:-1] here because I want to whack the % argument sent to the database.
    keyword_counter_list.append((search_words[1:-1], job_title_result_count))
    print(f'\t{job_title_result_count} jobs found')
    print(f'\tstarting word frequency aggregation')
    # I chose to run eaach job indivudally then sum them. I made an aggregator function
    job_title_word_count_df = description_word_frequency_aggregator(job_title_result_set)
    # I had to throw this in because calculating the length on an empty datframe was throwing a Value Error.
    if not job_title_word_count_df.empty:
        job_title_word_count_df['length'] = job_title_word_count_df.apply(get_the_length, axis=1)
    print(f'\tword frequency aggregation ended. ')
    print(f'\tstart pulling source data')
    # create a data frame of all of the jobs which mach the terms in the source title.
    print(f'\tcreate a data frame of all of the jobs which mach the terms in the source title.')
    job_title_source_data = pd.read_sql(
        sql=swr.query(SiSr).filter(SiSr.job_title.ilike(search_words)).order_by(SiSr.publish_date.desc()).statement,
        con=swr.bind)
    print(f'\tSource jobs pulled')
    # need top combine the phrases for the filename
    search_phrase = search_words[1:-1]
    print(f'\tBegin excel output')

    excel_sheet_1 = f'word_{search_phrase}_in_title'[:30]
    excel_sheet_2 = f'job_{search_phrase}_in_title'[:30]
    filename = f'.\output\{search_phrase}-{randrange(1, 100)}.xlsx'
    with pd.ExcelWriter(filename, engine='xlsxwriter', mode='a+') as my_excel_homeboy:
        job_title_word_count_df.to_excel(my_excel_homeboy, sheet_name=excel_sheet_1[0:30], index_label='Rank')
        job_title_source_data.to_excel(my_excel_homeboy, sheet_name=excel_sheet_2[0:30], index_label='Rank')
    my_excel_homeboy.save()
    print(f'\tEnd Excel output')
    print(f'\t{search_phrase} complete')

comparison_filename = f'all_searches_compared-{randrange(1, 100)}.xlsx'
keyword_count_df = pd.DataFrame(keyword_counter_list, columns=['search phrase', 'number of jobs returned'])
# I found that I was always sorting by total jobs descending so I put it in here.
keyword_count_df.sort_values('number of jobs returned', inplace=True, ascending=False)
with pd.ExcelWriter(f'.\output\{comparison_filename}', engine='xlsxwriter', mode='a+') as the_comparison_file:
    keyword_count_df.to_excel(the_comparison_file, sheet_name=comparison_filename[0:30], index=False)

session_with_remulak.close()
