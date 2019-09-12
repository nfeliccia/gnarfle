import pandas as pd
from sqlalchemy import create_engine

import blunt_skull_tools as bst

db_string = bst.create_pg_login_string()
db_six_cyl_engine = create_engine(db_string, echo=False)
iss_template_filename = r'iss_template.xlsx'
iss_template_df = pd.read_excel(iss_template_filename)
print(iss_template_df)
iss_template_df = iss_template_df.astype({'search_keyword_list': str, 'search_zip_code': str})
iss_template_df['search_run_date'] = pd.to_datetime(iss_template_df['search_run_date'], format='%m/%d/%Y')
iss_template_df.to_sql('indeed_search_set', con=db_six_cyl_engine, if_exists='append', index=False)
