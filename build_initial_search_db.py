print(initial_search_db.dtypes)
initial_search_db.to_sql('indeed_search_set', db_six_cyl_engine, if_exists='append', index=False,
                         dtype={'search_keyword_list': sqlalchemy.types.NVARCHAR(length=63),
                                'creation_date': sqlalchemy.DateTime(), 'search_completed': sqlalchemy.types.Boolean,
                                'search_run_date': sqlalchemy.types.DateTime()})
