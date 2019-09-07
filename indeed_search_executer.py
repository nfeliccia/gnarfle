from sqlalchemy import Integer, String, MetaData, Table, Column, create_engine, text, inspect

from aws_login_credentials import awlc


def this_is_an_example_block_on_injecting_sql(in_engine):
    """
    This serves as a blcok of reference code
    Executing via the engine directly is called connectionless execution. The engine connects and disconnects for us
    Using a connection is called explicit execution We control the span of a connection use

    :param in_engine:
    :return:
    """
    with in_engine.connect() as con:
        some_new_rows = ({'search_keyword_list': 'Administrative Assistant', 'search_zip_code': '19111',
                          'creation_date': '08/18/2019', 'search_completed': False},
                         {'search_keyword_list': 'Key Holder', 'search_zip_code': '19115',
                          'creation_date': '08/18/2019', 'search_completed': False},
                         {'search_keyword_list': 'Insurance Adjuster', 'search_zip_code': '19087',
                          'creation_date': '08/18/2019', 'search_completed': False})
        sql_pt_1 = 'INSERT INTO indeed_search_set(search_keyword_list,search_zip_code,creation_date,search_completed) '
        sql_pt_2 = 'VALUES (:search_keyword_list, :search_zip_code, :creation_date,:search_completed)'
        full_query = text(sql_pt_1 + sql_pt_2)
        for row_tuple in some_new_rows:
            db_six_cyl_engine.execute(full_query, **row_tuple)
        con.close()


def create_the_zip_code_table_do_not_run(in_engine):
    metadata = MetaData()
    zip_code_table = Table('ref_zip_code', metadata, Column('id', Integer, primary_key=True), Column('metro', String),
                           Column('zip_code', String), Column('rank', Integer))
    # WE assigned the name to the table in the first positional parameter
    print(f"zip_code_table.name\t{zip_code_table.name}\ttype {type(zip_code_table.name)}")
    # A table.c addresses the columns and returns a <class 'sqlalchemy.sql.base.ImmutableColumnCollection'>
    print(f"zip_code_table.c\t{zip_code_table.c}\ttype {type(zip_code_table.c)}")
    # Columns are .addressable as a column type
    print(f"zip_code_table.c.metro\t{zip_code_table.c.metro}\ttype {type(zip_code_table.c.metro)}")
    # and you can drill down to the name parameter on column
    print(f"zip_code_table.c.metro.name\t{zip_code_table.c.metro.name}\ttype {type(zip_code_table.c.metro.name)}")
    # A table.columns addresses the columns and returns a <class 'sqlalchemy.sql.base.ImmutableColumnCollection'>
    print(f"zip_code_table.columns\t{zip_code_table.columns}\ttype {type(zip_code_table.columns)}")
    zctck = zip_code_table.columns.keys()
    print(f"zip_code_table.columns.keys()\t{zctck}\ttype {type(zctck)}")
    zctczt = zip_code_table.columns.zip_code.type
    print(f"zip_code_table.columns.zip_code.type\t{zctczt}\ttype {type(zctczt)}")
    zctczn = zip_code_table.columns.zip_code.name
    print(f"zip_code_table.columns.zip_code.name\t{zctczn}\ttype {type(zctczn)}")
    zctcztb = zip_code_table.columns.zip_code.table
    print(f"zip_code_table.columns.zip_code.table\t{zctcztb}\ttype {type(zctcztb)}")
    zctpk = zip_code_table.primary_key
    print(f"zip_code_table.columns.zip_code.primary_key\t{zctpk}\ttype {type(zctpk)}")
    # and you can evan make a ready made select statement ah ha!
    print(f"zip_code_table.select()\t{zip_code_table.select()}\ttype {type(zip_code_table.select())}")
    metadata.create_all(db_six_cyl_engine)
    """
    This is the code that crated ref_zip_code table- 
    It continues to be included as a reference on how to use the create table commands. 
    """
    metadata = MetaData()
    zip_code_table = Table('ref_zip_code', metadata, Column('id', Integer, primary_key=True), Column('metro', String),
                           Column('zip_code', String), Column('rank', Integer))
    # WE assigned the name to the table in the first positional parameter
    print(f"zip_code_table.name\t{zip_code_table.name}\ttype {type(zip_code_table.name)}")
    # A table.c addresses the columns and returns a <class 'sqlalchemy.sql.base.ImmutableColumnCollection'>
    print(f"zip_code_table.c\t{zip_code_table.c}\ttype {type(zip_code_table.c)}")
    # Columns are .addressable as a column type
    print(f"zip_code_table.c.metro\t{zip_code_table.c.metro}\ttype {type(zip_code_table.c.metro)}")
    # and you can drill down to the name parameter on column
    print(f"zip_code_table.c.metro.name\t{zip_code_table.c.metro.name}\ttype {type(zip_code_table.c.metro.name)}")
    # A table.columns addresses the columns and returns a <class 'sqlalchemy.sql.base.ImmutableColumnCollection'>
    print(f"zip_code_table.columns\t{zip_code_table.columns}\ttype {type(zip_code_table.columns)}")
    zctck = zip_code_table.columns.keys()
    print(f"zip_code_table.columns.keys()\t{zctck}\ttype {type(zctck)}")
    zctczt = zip_code_table.columns.zip_code.type
    print(f"zip_code_table.columns.zip_code.type\t{zctczt}\ttype {type(zctczt)}")
    zctczn = zip_code_table.columns.zip_code.name
    print(f"zip_code_table.columns.zip_code.name\t{zctczn}\ttype {type(zctczn)}")
    zctcztb = zip_code_table.columns.zip_code.table
    print(f"zip_code_table.columns.zip_code.table\t{zctcztb}\ttype {type(zctcztb)}")
    zctpk = zip_code_table.primary_key
    print(f"zip_code_table.columns.zip_code.primary_key\t{zctpk}\ttype {type(zctpk)}")
    # and you can evan make a ready made select statement ah ha!
    print(f"zip_code_table.select()\t{zip_code_table.select()}\ttype {type(zip_code_table.select())}")
    metadata.create_all(in_engine)


# Read the user and password file.
login_file = 'login_info.json'
# The log
# The login credentials tuple contains db_string, username, password, working_db
# the ask ok flag bypasses the asking if the login credentials are OK becuase
# continually entering y in development is a pain in the butt.
login_credentials_tuple = awlc(login_file, askok=False)
db_string = login_credentials_tuple[0]
user_id = login_credentials_tuple[1]
password = login_credentials_tuple[2]
working_db = login_credentials_tuple[3]
# create the string from the variables and create the engine
db_string = f'postgres+psycopg2://{user_id}:{password}@{db_string}:5432/{working_db}'
db_six_cyl_engine = create_engine(db_string, echo=True)

metadata2 = MetaData()
indeed_search_set_reflected = Table('indeed_search_set', metadata2, autoload=True, autoload_with=db_six_cyl_engine)
inspector=inspect(db_six_cyl_engine)
ref_zip_code_columns=(inspector.get_columns('ref_zip_code'))
for column in ref_zip_code_columns:
    print(column)

