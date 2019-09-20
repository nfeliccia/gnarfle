"""
This file contains code used once for a specific purose.


"""
import pandas as pd

import gnarfle_00_bst_tools as bst


def create_indeed_search_queue():
    """
    2019-09-17 Used to convert from the talbe

    :return:a table on the postgres database
    """

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
        return pd.read_excel(in_excel_file, in_excel_sheet)

    excel_file = 'indeed_search_set.xlsx'
    excel_sheet = 'indeed_search_set'
    indeed_search_queue_df = read_an_excel_sheet(excel_file, excel_sheet)
    session_with_remulak_raes = bst.start_a_sql_alchemy_session()
    indeed_search_queue_df.to_sql('indeed_search_queue', con=session_with_remulak_raes.bind, index=True)


def create_the_zip_codes_table():
    session_with_remulak = bst.start_a_sql_alchemy_session()
    zipcodes_df = pd.read_excel('zipcodes.xlsx').astype({'zip_code': str})
    del zipcodes_df['Text']
    zipcodes_df.to_sql('ref_zip_codes',session_with_remulak.bind,index=True,index_label='zipcodes_pk')

def whack_words_table():
    session_with_remulak = bst.start_a_sql_alchemy_session()
    zipcodes_df = pd.read_excel('whack_words.xlsx').astype({'whack_word': str})
    zipcodes_df.to_sql('ref_whack_words',session_with_remulak.bind,index=True,index_label='whackword_pk')



whack_words_table()