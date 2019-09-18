import gnarfle_00_bst_tools as bst
from gnarfle_00_bst_tools import IndeedSearchQueue as ISQ

session_with_remulak = bst.start_a_sql_alchemy_session()

isq_search_set = session_with_remulak.query(ISQ).filter(ISQ.search_completed == False).order_by(ISQ.creation_date).all()
print (isq_search_set)
for isq_result in isq_search_set:
    print(isq_result.__dict__)