import pandas as pd
from sqlalchemy import create_engine
import datetime

from aws_login_credentials import awlc


def create_pg_login_string():
    login_file = 'login_info.json'
    login_credentials_tuple = awlc(login_file, askok=False)
    db_string = login_credentials_tuple[0]
    user_id = login_credentials_tuple[1]
    password = login_credentials_tuple[2]
    working_db = login_credentials_tuple[3]
    return f'postgres+psycopg2://{user_id}:{password}@{db_string}:5432/{working_db}'


db_string = create_pg_login_string()
db_six_cyl_engine = create_engine(db_string, echo=False)

iss_template_filename = r'iss_template.csv'
iss_template_df = pd.read_csv(iss_template_filename, encoding='utf-8')
print(iss_template_df)

iss_template_df.to_sql('indeed_search_set',con=db_six_cyl_engine,if_exists='append')
print (type(datetime.datetime.now()))
