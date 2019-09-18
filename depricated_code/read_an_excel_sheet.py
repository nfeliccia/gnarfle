def read_an_excel_sheet(in_excel_file, in_excel_sheet):
    """
    # the information to be searched is read off the indeed_search_set.xlsx. I use excel instead of
    # csv because excel keeps and passes datatype.
    in the future a more elegant interface can be built.
     There are multiple rows in this , so I set up an iterable to go through each of the rows.
    :param in_excel_file:
    :param in_excel_sheet
    :return: an iterrows dataframe object
    """
    return pd.read_excel(in_excel_file, in_excel_sheet).iterrows()