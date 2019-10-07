from collections import namedtuple
from itertools import permutations
from random import randrange

import pandas as pd

from gnarfle_bst import SQLIndeedSearchResults
from gnarfle_bst import cls
from gnarfle_bst import start_a_sql_alchemy_session
from gnarfle_bst import super_clean_a_string

cls()
session_with_remulak = start_a_sql_alchemy_session()
scramble_list_words = []
scramble_results_tuple_list = []
scramble_phrase_list = []
keep_getting_phrases = True
ScrambleResult = namedtuple('ScrambleResult', ['scramble_phrase', 'ocurances_in_title', 'ocurances_in_text'],
                            rename=True)

print("Type 'quit' to stop entering phrases\n")
while keep_getting_phrases:
    scramble_phrase = input(f"Get phrase to scramble:\t")
    scramble_phrase = super_clean_a_string(scramble_phrase)
    if scramble_phrase == 'quit':
        print("That's all the input you get! ")
        keep_getting_phrases = False
    else:
        scramble_split = scramble_phrase.split()
        for i in range(1, len(scramble_split) + 1):
            scramble_permutations = permutations(scramble_split[:i])
            scramble_phrase_list.extend([' '.join(group[0:3]) for group in scramble_permutations])

scramble_phrase_set = set(scramble_phrase_list)
for scramble_phrase in scramble_phrase_set:
    scramble_words_sql = f'%{scramble_phrase}%'
    scramble_list_words_query = session_with_remulak.query(SQLIndeedSearchResults.job_title).filter(
        SQLIndeedSearchResults.job_title.ilike(scramble_words_sql))
    scramble_list_words_query_count = scramble_list_words_query.count()
    scramble_list_body_query = session_with_remulak.query(SQLIndeedSearchResults.job_text_raw).filter(
        SQLIndeedSearchResults.job_text_raw.ilike(scramble_words_sql))
    scramble_list_body_query_count = scramble_list_body_query.count()
    ThisResult = ScrambleResult(scramble_phrase=f'{scramble_phrase}',
                                ocurances_in_title=scramble_list_words_query_count,
                                ocurances_in_text=scramble_list_body_query_count)
    print(
        f'Scramble Phrase = {ThisResult.scramble_phrase} \t Title Occurances {getattr(ThisResult, "ocurances_in_title")} \t Text Occurances {ThisResult[2]}\n')
    scramble_results_tuple_list.append(ThisResult)

scramble_df = pd.DataFrame(scramble_results_tuple_list)
scramble_df.sort_values(by=['ocurances_in_title', 'ocurances_in_text', 'scramble_phrase'], ascending=False,
                        inplace=True)
scramble_df.drop_duplicates(inplace=True)

comparison_filename = f'permutations_compared-{randrange(1, 100)}.xlsx'
print(f'\n\nDataFrame\n Written to {comparison_filename}\n', scramble_df)
with pd.ExcelWriter(f'.\output\{comparison_filename}', engine='xlsxwriter', mode='a+') as the_comparison_file:
    scramble_df.to_excel(the_comparison_file, sheet_name=comparison_filename[0:30], index=False)
session_with_remulak.close()
