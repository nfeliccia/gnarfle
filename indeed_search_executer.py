"""
indeed_search_executer.py goes through the database table indeed_search_set. This table holds the two critical elements
for an indeed.com search, the search keyword list and the search zip code. It also has a seach_completed flag which is
flipped when the search is successfully completed.

"""
import blunt_skull_tools as bst
from indeed_search_function import isf
def indeed_search_executer():
    session_with_remulak = bst.start_a_sql_alchemy_session()
    # First thing is query the table for everything where search completed is true
    yet_to_be_done_searches = session_with_remulak.query(bst.Serch).filter_by(search_completed=False).order_by(
        'iss_pk').all()
    # Iterate through the uncompleted results and send each one to the search
    [isf(search) for search in yet_to_be_done_searches]
    # commit and quit!
    session_with_remulak.commit()
    session_with_remulak.close()
