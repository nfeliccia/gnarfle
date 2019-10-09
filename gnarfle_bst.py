import re
from collections import Counter
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString
from nltk import PorterStemmer
from nltk import ngrams
from nltk import word_tokenize
from nltk.corpus import stopwords
from sqlalchemy import Integer, String, Date, Float, Column, Boolean
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session

from aws_login_credentials import awlc

Base = declarative_base()


class IndeedSearchQueue(Base):
    __tablename__ = 'indeed_search_queue'
    isq_pk = Column(Integer, primary_key=True)
    search_keyword_list = Column(String)
    creation_date = Column(Date)
    search_completed = Column(Boolean)
    search_run_date = Column(Date)
    search_zip_code = Column(String)

    def __init__(self):
        self.isq_pk = None
        self.search_keyword_list = ""
        self.creation_date = None
        self.search_run_date = None
        self.search_zip_code = None


class IndeedJobDescriptionAnalysis:
    def __init__(self):
        self.job_title = "unknown"
        self.job_text_raw = ""

    def populate(self, in_soup):
        self.job_title = in_soup.h3.text
        self.job_text_raw = in_soup.find('div', {'id': 'jobDescriptionText'}).get_text()


class IndeedJobSearchResult:
    def __init__(self):
        self.job_title_row = ""
        self.extracted_url = ""
        self.company = ""
        self.guid = ""
        self.latitude = 0.0
        self.longitude = 0.0
        self.publish_date = datetime.now()
        self.scraped = False
        self.iss_pk = 0

    def populate(self, in_job_result):
        self.job_title_row = in_job_result.title.text
        self.extracted_url = trim_indeed_url(in_job_result.contents[2])
        self.company = in_job_result.contents[3].text
        self.publish_date = datetime.strptime(in_job_result.contents[5].string, f'%a, %d %b %Y %H:%M:%S GMT')
        self.guid = in_job_result.contents[4].text
        self.latitude = float(in_job_result.contents[7].string.split(' ')[0])
        self.longitude = float(in_job_result.contents[7].string.split(' ')[1])


class SQLIndeedSearchResults(Base):
    __tablename__ = 'indeed_search_results'
    isr_pk = Column(Integer, primary_key=True)
    company = Column(String)
    extracted_url = Column(String)
    guid = Column(String)
    job_title_row = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    publish_date = Column(Date)
    job_text_raw = Column(String)
    job_title = Column(String)

    def populate(self, in_result, in_analysis):
        self.company = in_result.company
        self.extracted_url = in_result.extracted_url
        self.guid = in_result.guid
        self.job_title_row = in_result.job_title_row
        self.latitude = in_result.latitude
        self.longitude = in_result.longitude
        self.publish_date = in_result.publish_date
        self.job_text_raw = in_analysis.job_text_raw
        self.job_title = in_analysis.job_title


class WhackWords(Base):
    __tablename__ = 'ref_whack_words'
    ww_pk = Column(Integer, primary_key=True)
    whack_word = Column(String)


class ZipCodes(Base):
    __tablename__ = 'ref_zip_codes'
    zipcodes_pk = Column(Integer, primary_key=True)
    zip_code = Column(String)
    metro_area_name = Column(String)
    metro_area_rank = Column(Float)


def bs_showit(in_bsthing, print_it_out=False):
    if print_it_out:
        if isinstance(in_bsthing, NavigableString):
            print(in_bsthing.string)
        else:
            print(in_bsthing.text)
    else:
        if isinstance(in_bsthing, NavigableString):
            return in_bsthing.string
        else:
            return in_bsthing.text


def build_on_the_indeed_search_list(in_session):
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

    search_word_bank = get_big_set_of_search_words_for_indeed()
    search_depth = how_deep_to_search_in_geographic_list()

    # get the list of zip_codes - return a class.
    zip_code_query = in_session.query(ZipCodes).filter(ZipCodes.metro_area_rank < search_depth)
    zip_code_list = zip_code_query.all()
    for set_of_search_words in search_word_bank:
        for zip_code_class in zip_code_list:
            this_indeed_search_row = IndeedSearchQueue()
            this_indeed_search_row.search_keyword_list = set_of_search_words
            this_indeed_search_row.creation_date = datetime.now()
            this_indeed_search_row.search_completed = False
            this_indeed_search_row.search_zip_code = zip_code_class.zip_code
            in_session.add(this_indeed_search_row)

    # print(zip_codes)
    print("Writing to search queue...")
    in_session.commit()
    print("... search queue update complete")
    return search_word_bank

def cls():
    print(f'\n'*40)

def create_headers_for_the_browser():
    # headers for the Browser.
    # split up to keep on one line.
    user_agent_pt_1 = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    user_agent_pt_2 = '(KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
    return {'User-Agent': user_agent_pt_1 + user_agent_pt_2}


def create_pg_login_string():
    """
    This function takes no input, reads the login file, parses the information and
    concatenates it into a string for the postress engine to use.
    :return: string
    """
    # The login credentials tuple contains db_string, username, password, working_db
    # the ask ok flag bypasses the asking if the login credentials are OK becuase
    # continually entering y in development is a pain in the butt.
    login_file = 'login_info.json'
    login_credentials_tuple = awlc(login_file, askok=False)
    aws_db_url = login_credentials_tuple[0]
    user_id = login_credentials_tuple[1]
    password = login_credentials_tuple[2]
    working_db = login_credentials_tuple[3]
    return f'postgres+psycopg2://{user_id}:{password}@{aws_db_url}:5432/{working_db}'


def dedup_indeed_search_results(in_session: Session):
    """
    This is a routine which removes duplicates from the indeed search results. I put it here so
    it can be called anywhere.
    :param in_session:
    :return: None
    """
    # writing the SQL statements in   a list makes it easier to read and create separate pieces
    sql_statements = ['DELETE FROM indeed_search_results AS a USING (SELECT MIN(isr_pk) as isr_pk,',
                      'guid FROM indeed_search_results ',
                      'GROUP BY guid HAVING COUNT(*) > 1 ) AS b WHERE a.guid = b.guid', ' AND a.isr_pk <> b.isr_pk;']
    full_query = text(' '.join(sql_statements))
    in_session.execute(full_query)
    in_session.commit()


def description_word_frequency_aggregator(in_job_title_result_set: Query):
    """
    This function takes in the results for a query and aggregates the single description counter.
    :param in_job_title_result_set: Query
    :return: pandas data frame
    """
    counter_results_sum = Counter()
    print(f'\t\tStart building counter results list', end='\t')
    counter_results_list = [word_frequency_for_a_single_description(in_result.job_text_raw) for in_result in
                            in_job_title_result_set]
    print(f'\t\tfinished building counter results list')
    print(f'\t\tBeginning summation of counter results ', end='\t')
    # I use the .update command because its supposed to be the fastest to sum up counters.
    for counter_result in counter_results_list:
        counter_results_sum.update(counter_result)
    print(f'\t\tEnding summation of counter results')
    result_to_present = counter_results_sum.most_common(1300)
    print(f'\t\tBeginning conversion to data frame', end='\t')
    word_count_df = pd.DataFrame(result_to_present, columns=['keyword', 'number_of_hits'])
    print(f'\t\tEnd conversion to data frame')
    print(f'\t\tBeginning stopwords scrub', end='\t')
    word_count_df = stopwords_scrub(word_count_df)
    print(f'\t\tEnding stopwords scrub')
    return word_count_df


def get_big_set_of_search_words_for_indeed() -> list:
    """
    This loop gives us a list of search words to send to indeed_search_queue
    :return: list of strings
    """

    def get_single_set_of_search_words_for_indeed():
        """
        This function asks the user for search terms, whacks any strange characters, and then
        splits up the first three words. Any extra ones are ignored
        :return:A string of Keywords for an indeed search separated by a plus character
        """
        search_keywords = input("\nEnter Search Keyword List up to 3 words no punctuation\ntype 'quit' when done:\t")
        # I made a module called super clean a string which uses RE to get out special characters
        search_keywords = super_clean_a_string(search_keywords)
        # Split on spaces in case a special character was replaced with a space
        search_keyword_list = search_keywords.split(' ')
        # I rejoin with a plus because that's the search syntax for indeed.
        # I limit the first few elements of the list
        search_keywords_for_indeed = '+'.join(search_keyword_list[0:3])
        return search_keywords_for_indeed

    keep_getting_search_words = True
    search_word_bank_gbsoswfi = []
    while keep_getting_search_words:
        this_set_of_search_words = get_single_set_of_search_words_for_indeed()
        if this_set_of_search_words.find('quit') > -1:
            keep_getting_search_words = False
        else:
            search_word_bank_gbsoswfi.append(this_set_of_search_words)
    return search_word_bank_gbsoswfi


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


def move_single_job_result_from_indeed_to_database(in_single_job_result, in_bs_session, in_swr):
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
        detailed_job_description_page_tree = in_bs_session.get(in_job_desc_url,
                                                               headers=create_headers_for_the_browser())
        return BeautifulSoup(detailed_job_description_page_tree.content, 'lxml')

        # Create an instance and build the data in the ISJR Class with the inbuilt method called populate

    this_is_jr_result = IndeedJobSearchResult()
    this_is_jr_result.populate(in_single_job_result)
    # Take the job URL and scrape it for the text.
    detailed_job_description_page_soup = create_job_description_soup(this_is_jr_result.extracted_url, )
    # Create an instance of the Indeed Job descripiton Analysis info.
    this_indeed_job_description_analysis = IndeedJobDescriptionAnalysis()
    this_indeed_job_description_analysis.populate(detailed_job_description_page_soup)
    # creaet an incidence of the SQL Alchemy based class SQLIndeedSearchResults
    this_sql_upload = SQLIndeedSearchResults()
    this_sql_upload.populate(this_is_jr_result, this_indeed_job_description_analysis)
    in_swr.add(this_sql_upload)


def query_indeed_and_grab_a_jobs_result_set(in_isq, in_page_num):
    """
    This block creates the URL to send to indeed, uses Beautiful Soup to get the
    :param in_isq: Pandas data series
    :param in_page_num: integer
    :return: beautiful Soup result set
    """
    beautiful_soup_session = requests.Session()
    indeed_url_part_1 = f"http://rss.indeed.com/rss?q={chr(34)}{in_isq.search_keyword_list}{chr(34)}"
    indeed_url_part_2 = f'&l={in_isq.search_zip_code}&start={str(in_page_num)}'
    search_results_page = f"{indeed_url_part_1}{indeed_url_part_2}"
    search_results_page_tree = beautiful_soup_session.get(search_results_page, headers=create_headers_for_the_browser())
    search_results_page_soup = BeautifulSoup(search_results_page_tree.content, 'lxml')
    return search_results_page_soup.find_all('item')

def show_off(in_file_name):
    with open(in_file_name, 'r', encoding='utf-8') as show_file_name:
        for line in show_file_name.readlines():
            print(line, end='')


def super_clean_a_string(in_string: str):
    out_string = re.sub(r'[,.;@%#?!&$()^*/\|’\':·\-]+\ *', " ", in_string).rstrip().lstrip().casefold()
    return out_string


def start_a_sql_alchemy_session():
    """ This starts  a session using SQL Alchemy tools
    """
    # initiate the database_session
    db_string = create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    database_session = sessionmaker()
    return database_session(bind=db_six_cyl_engine)


def stopwords_scrub(in_df: pd.DataFrame):
    """

    :param in_df: Pandas Dataframe
    :return:out_df: Pandas DAtaFrame
    """
    swr = start_a_sql_alchemy_session()
    # I start with the english stopwords
    words_to_whack = stopwords.words('english')
    # Query from the database the current stopword list. I'm keeping the stopword list on the db for easy uprade
    whack_words_query = swr.query(WhackWords.whack_word)
    # I combined the .all with the query and the list iterator because the query comes back as a list of tuples.
    whack_words_list = [wwql.whack_word for wwql in whack_words_query.all()]
    words_to_whack.extend(whack_words_list)
    # I sort so the shortest words and presumably the most frequent will be at
    words_to_whack.sort(key=len)
    # the beginning of the list and caught first.
    words_to_whack_series = pd.Series(words_to_whack)
    out_df = in_df[~in_df['keyword'].isin(words_to_whack_series)]
    out_df.reindex(copy=False)
    swr.close()
    return out_df


def trim_indeed_url(self):
    """
    When a URL is reutrned in the XML, it has a lot of extra characters in it.   There are several reasons we want to
    simplify the URL. 1) Remove any identifying information  2) Ease of Storage  Note, I do leave in the from=rss
    text because If Indeed is counting usage of the RSS, I don't want it to go away.
    :param self: text
    :return: out_url:text
    """
    jk_snippet = re.search('jk=', self).start()
    rtk_snippet = re.search('&rtk=', self).start()
    front_snippet = self[0:30]
    out_url = f'{front_snippet}{self[jk_snippet:rtk_snippet]}&from=rss'
    return out_url


def word_frequency_for_a_single_description(in_job_text: str):
    """
    The purpose of this function is to take in a raw string of job text scraped from the web,
    and get a frequency word count. I chose to return a counter object because they can be summed together.

    :param in_job_text: which is a string  of job text scraped from a job description
    :return: a Counter Object with teh word counts
    """
    our_stemmer = PorterStemmer()
    # remove all sorts of punctuation to optimize word parsing.
    job_text_string = super_clean_a_string(in_job_text)
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
    return job_desc_count
