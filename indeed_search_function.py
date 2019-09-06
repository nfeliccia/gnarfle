import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import os

""" The purpose of this module is to take in keywords, location and return a panndas df"""


def de_html_a_block(in_text):
    """re
    This block is here to remove any stray HTML that could be left in a description.
    I put in a print statement to see if it was called, and boy was it called 9/3/2019
    :param in_text:string
    :return: out_text:string
    """
    out_text = re.sub(r'<br>', '', in_text)
    return out_text


def desciption_clean(in_text):
    excess_snippet = re.search(' - ', in_text, re.UNICODE).start()
    mid_text = in_text[:excess_snippet]
    out_text = re.sub(fr'[^a-zA-Z0-9{chr(32)}\-]', '', mid_text)
    return out_text


def job_title_clean(in_text):
    out_text = re.sub(fr'[^a-zA-Z0-9{chr(32)}\-]', ' ', in_text)
    return out_text


def trim_indeed_url(in_url):
    """
    When a URL is reutrned in the XML, it has a lot of extra characters in it.   There are several reasons we want to
    simplify the URL. 1) Remove any identifying information  2) Ease of Storage  Note, I do leave in the from=rss
    text because If Indeed is counting usage of the RSS, I don't want it to go away.
    :param in_url: text
    :return: out_url:text
    """
    jk_snippet = re.search('jk=', in_url).start()
    rtk_snippet = re.search('&rtk=', in_url).start()
    front_snippet = in_url[0:30]
    out_url = f'{front_snippet}{in_url[jk_snippet:rtk_snippet]}&from=rss'
    return out_url


def squeeze_out_the_good_data(in_bs_thing):
    """
    This takes in the basic unit of xml for a job description Item and parses it out.
    The job title usually contans  Job Title - Posting Company - zip.
    I split this up
    the guid seems to be a unique id on search results which isused for de duping later and makes a convenient unique id
    At this point I'm just leaving the publish date as text and not parsing it.
    The short description should later be the meat of what we mine for keywords.
    Interestingly, indeed passes along the lat/long of the town which I split and convert to float
    All the information is concatenated into a list to be used later in DataFrame Creation
    :param in_bs_thing:Beautiful Soup .Tag Object
    :return: list with parsed data in columns for dataframe creation.
    """
    job_title = job_title_clean(in_bs_thing.title.text)
    # Split job title up into the 3 components
    job_title_split = job_title.split(' - ')
    real_job_title = job_title_split[0]
    company = job_title_split[1]
    search_zip = job_title_split[2]
    guid = in_bs_thing.guid.text[:10]
    source = in_bs_thing.source.text
    publish_date = in_bs_thing.pubdate.text
    short_decription = de_html_a_block(in_bs_thing.description.text)
    shorter_description = desciption_clean(short_decription)
    lat_lon = in_bs_thing.find('georss:point').text.split(' ')
    lat = float(lat_lon[0])
    long = float(lat_lon[1])
    extracted_url = trim_indeed_url(in_bs_thing.contents[2])
    all_the_columns = [guid, job_title, real_job_title, company, search_zip, source, publish_date, shorter_description,
                       lat, long, extracted_url]
    return all_the_columns


def indeed_rss_search(in_keywordz, in_location_zip):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    # The indeed listing shows 10 pages at a time. I set a page num max as a variable her on the number of results
    long_item_list = []
    data_frame_builder = []
    search_result_columns = ['guid', 'job_title_row', 'real_job_title', 'company', 'in_location_zip', 'listing_source',
                             'publish_date', 'short_description', 'lat', 'longitude', 'extracted_url', 'scraped']

    # Run through and grab the RSS for the key words and location 10 at a time
    # Append to a list
    print(f'downloading results for {in_keywordz}')
    page_num = 0
    results_still_coming = True
    while results_still_coming:
        session = requests.Session()
        page = f"http://rss.indeed.com/rss?q={chr(34)}{in_keywordz}{chr(34)}&l={in_location_zip}&start=" + str(page_num)
        pageTree = session.get(page, headers=headers)
        pageSoup = BeautifulSoup(pageTree.content, 'html5lib')
        rss = pageSoup.find_all('item')
        # Trap a null result and make the output an empty set
        # The page_num can be adjusted for the depth of results.
        if len(rss) == 0 or page_num > 1000:
            results_still_coming = False
        else:
            long_item_list.extend(rss)
            page_num += 10

    # if no results are found, throw error and then
    if len(long_item_list) < 1:
        raise ValueError(f'No Results Found for {in_keywordz}')
    else:
        for each_block in long_item_list:
            try:
                incremental_line = squeeze_out_the_good_data(each_block)
                incremental_line.append(False)
                data_frame_builder.append(incremental_line)
            except IndexError:
                print('Bad Data Handled')

    search_result_dataframe = pd.DataFrame.from_records(data_frame_builder, columns=search_result_columns)
    search_result_dataframe.drop_duplicates(['guid'], inplace=True)
    return search_result_dataframe


# -------------------------------------------------------

keywordz = 'data+analyst'
location_zip = '19124'
sr_df = indeed_rss_search(keywordz, location_zip)
print(sr_df.columns)
print(sr_df.head())
print(sr_df.shape)
print(sr_df.dtypes)
sr_df.to_csv('E:\Software\gnarfle\isf_tmp.csv')
