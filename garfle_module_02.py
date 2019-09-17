import re
from collections import Counter
from datetime import datetime as dt2

from nltk import PorterStemmer, ngrams, word_tokenize
from nltk.corpus import stopwords
from pandas import DataFrame, read_sql, ExcelWriter
from sqlalchemy.orm.query import Query

import gnarfle_00_bst_tools as bst
from gnarfle_00_bst_tools import SQLIndeedSearchResults as SISR

# I want to have stems available in the result counts.
our_stemmer = PorterStemmer()
# l oad in the stop words
stop_words = set(stopwords.words('english'))


def stopwords_scrub(in_counter: Counter):
    words_to_whack = set(stopwords.words('english'))
    out_counter = in_counter.copy()
    for element in out_counter.copy():
        if element in words_to_whack:
            del out_counter[element]
    return out_counter


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
    for phrase_length in [2, 3]:
        job_text_multiples = ngrams(job_text_words, phrase_length)
        for multiple in job_text_multiples:
            # turn the multiple from a tuple into a phrase.
            additional_phrase = ' '.join(multiple)
            job_text_multiples_list.append(additional_phrase)
    # add the multiples list on to the main job words. Using one list to save memory.
    job_text_words.extend(job_text_multiples_list)
    job_text_words.extend(job_text_stems)
    job_desc_count = Counter(job_text_words)
    return job_desc_count


def description_word_frequency_aggregator(in_result: Query):
    """
    This function takes in the results for a query and aggregates the single description counter.
    :param in_result: Query
    :return: pandas data frame
    """
    type(in_result)
    counter_results_list = [word_frequency_for_a_single_description(in_result.job_text_raw) for in_result in
                            job_title_result_set]
    counter_results_sum = sum(counter_results_list, Counter())
    counter_results_scrubbed = stopwords_scrub(counter_results_sum)
    result_to_present = counter_results_scrubbed.most_common(250)
    word_count_df = DataFrame(result_to_present, columns=['keyword', 'number_of_hits'])
    return word_count_df


# Start - --------------------
session_with_remulak = bst.start_a_sql_alchemy_session()
# I want to keep the understanding that we're having  a session with the remote database remulac
# but want to alias it since it makes the statements long.
swr = session_with_remulak
# before you pull results , make sure that the indeed search results are d-duped.
bst.dedup_indeed_search_results(swr)
# I set the filename up here so I can do multiple writes without having it change. 

search_set = [('%career%', '%counselor%'), ('%career%', '%coach%'), ('%career%', '%advisor%')]


for search_tuple in search_set:
    # I want to use SQL Alchemy - note the i like here makes it case insensitive.
    job_title_result_set = swr.query(SISR).filter(SISR.job_title.ilike(search_tuple[0])).filter(
        SISR.job_title.ilike(search_tuple[1])).order_by(SISR.publish_date.desc())
    job_title_word_count_df = description_word_frequency_aggregator(job_title_result_set)

    job_title_source_data = read_sql(sql=swr.query(SISR).filter(SISR.job_title.ilike(search_tuple[0])).filter(
        SISR.job_title.ilike(search_tuple[1])).order_by(SISR.publish_date.desc()).statement, con=swr.bind)

    job_description_result_set = swr.query(SISR).filter(SISR.job_text_raw.ilike(search_tuple[0])).filter(
        SISR.job_text_raw.ilike(search_tuple[1]))

    search_phrase = search_tuple[0][1:-1] + '_' + search_tuple[1][1:-1]

    excel_sheet_1 = f'word_{search_phrase}_in_title'
    excel_sheet_2 = f'job_{search_phrase}_in_title'
    filename = f'{dt2.now().year}-{dt2.now().month}-{dt2.now().day}-{dt2.now().hour}-{dt2.now().minute}-{search_phrase}.xlsx'
    with ExcelWriter(filename, engine='xlsxwriter', mode='a+') as my_excel_homeboy:
        job_title_word_count_df.to_excel(my_excel_homeboy, sheet_name=excel_sheet_1[0:30])
        job_title_source_data.to_excel(my_excel_homeboy, sheet_name=excel_sheet_2[0:30])
        my_excel_homeboy.save()
    print(f'{search_tuple} complete')

session_with_remulak.close()
