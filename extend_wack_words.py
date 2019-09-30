import pandas as pd

from gnarfle_bst import start_a_sql_alchemy_session
from gnarfle_bst import WhackWords


def extend_whack_words():
    new_whack_words=WhackWords
    session_with_remulak = start_a_sql_alchemy_session()
    ww_new = pd.read_excel(r'.\input\whack_words_contributor.xlsx', )
    print(ww_new)
    session_with_remulak.bulk_insert_mappings(WhackWords,ww_new.to_dict(orient='records'))
    session_with_remulak.commit()
    session_with_remulak.close()
