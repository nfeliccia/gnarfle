from gnarfle_bst import get_big_set_of_search_words_for_indeed
from gnarfle_bst import start_a_sql_alchemy_session

session_with_remulak = start_a_sql_alchemy_session()

surchur_list = get_big_set_of_search_words_for_indeed()

print (surchur_list)




session_with_remulak.close()

