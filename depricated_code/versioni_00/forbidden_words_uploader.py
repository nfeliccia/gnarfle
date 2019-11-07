import pandas as pd

import blunt_skull_tools as bst
from sqlalchemy import create_engine

db_string = bst.create_pg_login_string()
db_six_cyl_engine = create_engine(db_string, echo=False)
forbidden_words_df = pd.read_csv('forbidden_words_list.csv')
print(forbidden_words_df)
forbidden_words_df.to_sql('ref_forbidden_words',con=db_six_cyl_engine,index_label='fw_pk',if_exists='replace')