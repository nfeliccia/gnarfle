import datetime
from collections import Counter
from random import randrange

import pandas as pd
from nltk import PorterStemmer, ngrams, word_tokenize
from nltk.corpus import stopwords
from sqlalchemy.orm.query import Query

import gnarfle_00_bst_tools as bst
from gnarfle_00_bst_tools import SQLIndeedSearchResults as SISR



def description_word_frequency_aggregator(in_result: Query):
    """
    This function takes in the results for a query and aggregates the single description counter.
    :param in_result: Query
    :return: pandas data frame
    """
    counter_results_sum = Counter()
    print(f'\t\tStart building counter results list {datetime.datetime.now() - start_time}')
    counter_results_list = [word_frequency_for_a_single_description(in_result.job_text_raw) for in_result in
                            job_title_result_set]
    print(f'\t\tfinished building counter results list {datetime.datetime.now() - start_time}')
    print(f'\t\tBeginning summation of counter results {datetime.datetime.now() - start_time} ')
    # I use the .update command because its supposed to be the fastest to sum up counters.
    for counter_result in counter_results_list:
        counter_results_sum.update(counter_result)
    print(f'\t\tEnding summation of counter results {datetime.datetime.now() - start_time}')
    print(f'\t\tBeginning stopwords scrub {datetime.datetime.now() - start_time}')
    counter_results_scrubbed = stopwords_scrub(counter_results_sum)
    print(f'\t\tEnding stopwords scrub {datetime.datetime.now() - start_time}')
    result_to_present = counter_results_scrubbed.most_common(1000)
    print(f'\t\tBeginning conversion to data frame {datetime.datetime.now() - start_time}')
    word_count_df = pd.DataFrame(result_to_present, columns=['keyword', 'number_of_hits'])
    print(f'\t\tEnd conversion to data frame {datetime.datetime.now() - start_time}')
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


# Start - --------------------

# I want to have stems available in the result counts.
our_stemmer = PorterStemmer()
# load in the stop words
stop_words = set(stopwords.words('english'))
start_time = datetime.datetime.now()

session_with_remulak = bst.start_a_sql_alchemy_session()
# I want to keep the understanding that we're having  a session with the remote database remulac
# but want to alias it since it makes the statements long.
swr = session_with_remulak
# before you pull results , make sure that the indeed search results are d-duped.
bst.dedup_indeed_search_results(swr)
# I set the filename up here so I can do multiple writes without having it change. 

search_set = bst.get_big_set_of_search_words_for_indeed()

keyword_counter_list = []
for search_phrase in search_set:
    # I had to put this in because too short of a search phrase returns too many darn results
    search_words = f'%{search_phrase.replace("+", " ")}%'
    if len(search_words) < 3:
        break
    # this is a quick and dirty way of handling less than three elements
    print(f'Testing {search_words}')
    print(f'\tFiltering {search_words} ')
    # I want to use SQL Alchemy - note the ilike command here makes it case insensitive.
    job_title_result_query = swr.query(SISR.job_title, SISR.job_text_raw).filter(
        SISR.job_title.ilike(search_words)).order_by(SISR.publish_date.desc())
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
        sql=swr.query(SISR).filter(SISR.job_title.ilike(search_words)).order_by(SISR.publish_date.desc()).statement,
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
