from datetime import datetime
from random import randrange

import pandas as pd
import requests

from gnarfle_bst import IndeedSearchQueue
from gnarfle_bst import SQLIndeedSearchResults
from gnarfle_bst import build_on_the_indeed_search_list
from gnarfle_bst import dedup_indeed_search_results
from gnarfle_bst import description_word_frequency_aggregator
from gnarfle_bst import get_big_set_of_search_words_for_indeed
from gnarfle_bst import get_the_length
from gnarfle_bst import move_single_job_result_from_indeed_to_database
from gnarfle_bst import query_indeed_and_grab_a_jobs_result_set
from gnarfle_bst import start_a_sql_alchemy_session
from gnarfle_bst import super_clean_a_string

# ---- BEGINNING of Program
# Set up one and only one BS session
beautiful_soup_session = requests.Session()
session_with_remulak = start_a_sql_alchemy_session()
# list initialization
keyword_counter_list = []
this_session_words = []
# I alias this to make shorter queries.
SiSr = SQLIndeedSearchResults
# I want to have stems available in the result counts.

# load in the stop words


# I start off by asking the user to augment the current search list, this gives the option to go right to the
# counting section.
work_the_search_list_user_input = input("Do you want to add to the current search list: ")
work_the_search_list_user_input_clean = super_clean_a_string(work_the_search_list_user_input)
if work_the_search_list_user_input_clean == 'y':
    this_session_words = build_on_the_indeed_search_list(session_with_remulak)

# I keep the list of pending searches on the database so the work may oneday be distributed
isq_search_set_query = session_with_remulak.query(IndeedSearchQueue).filter(
    IndeedSearchQueue.search_completed == False).order_by(IndeedSearchQueue.creation_date)
# I want to always test for pending items
if isq_search_set_query.count() > 0:
    isq_search_set = isq_search_set_query.all()
    # Loop through every result in the search set where its not been completeda
    for isq_search in isq_search_set:
        page_num = 0
        keep_searching = True
        # I want to give occasional status messages, but not so many they overwhelm
        print(f'working {isq_search.search_keyword_list} {isq_search.search_zip_code}', end='\t')
        try:
            while keep_searching:
                job_result_set = query_indeed_and_grab_a_jobs_result_set(isq_search, page_num)
                # check to make sure positive results are still being received
                if len(job_result_set) < 2 or page_num > 100:
                    keep_searching = False
                else:
                    page_num += 10
                for single_job_result in job_result_set:
                    move_single_job_result_from_indeed_to_database(single_job_result, beautiful_soup_session,
                                                                   session_with_remulak)
        except AttributeError as oh_skyte_1:
            print("Bad Read", end='\t')
            with open('logfile.txt', 'a+', encoding='utf-8') as out_log_file:
                out_log_file.write(str(oh_skyte_1))

        isq_search.search_completed = True
        print(f"Completed {isq_search.search_keyword_list} {isq_search.search_zip_code}")
        isq_search.search_run_date = datetime.now()

    print(f'Writing Search List to Database ...  ')
    session_with_remulak.commit()
    print(f'after  commit ')

# I want to keep the understanding that we're having  a session with the remote database remulac
# but want to alias it since it makes the statements long.
swr = session_with_remulak
# before you pull results , make sure that the indeed search results are d-duped.
dedup_indeed_search_results(swr)
# I set the filename up here so I can do multiple writes without having it change. 

if not this_session_words:
    search_set = get_big_set_of_search_words_for_indeed()
else:
    search_set = this_session_words.copy()

for search_phrase in search_set:
    # I had to put this in because too short of a search phrase returns too many darn results
    search_words = f'%{search_phrase.replace("+", " ")}%'
    if len(search_words) < 3:
        break
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
    else:
        job_title_word_count_df['length'] = None
    # The most useful way is to have it sorted by hits then by length
    job_title_word_count_df.sort_values(by=['number_of_hits', 'length'], ascending=False, inplace=True)
    job_title_word_count_df.reset_index(drop=True,inplace=True)
    print(f'\tword frequency aggregation ended. ')
    print(f'\tstart pulling source data')
    # create a data frame of all of the jobs which mach the terms in the source title.
    print(f'\tcreate a data frame of all of the jobs which mach the terms in the source title.')
    job_title_source_data = pd.read_sql(
        sql=swr.query(SiSr.company, SiSr.job_title_row, SiSr.publish_date, SiSr.job_title, SiSr.extracted_url,
                      SiSr.job_text_raw).filter(SiSr.job_title.ilike(search_words)).order_by(
            SiSr.publish_date.desc()).statement, con=swr.bind)
    print(f'\tSource jobs pulled')
    # need top combine the phrases for the filename
    search_phrase = search_words[1:-1]
    print(f'\tBegin excel output')
    # ouput the results to an excel file
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

# KEEP closes at end of program.
session_with_remulak.close()
beautiful_soup_session.close()
