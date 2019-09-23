import json
import pathlib

"""
This module looks for password jason in the root file. 
In the future this information will need to be encrypted 
And the password input can be upgraded to use the mypass function
"""


def read_password_json_file(in_pw_file_name):
    """
    :param: a string which is the input of a file name
    :return: tuple of database string, user Id and password
    """
    with open(in_pw_file_name, 'r', encoding='utf8') as pw_file_object_rpjf:
        pwd_json_object = json.load(pw_file_object_rpjf)
        db_string_rpjf = pwd_json_object["db_string"]
        user_id_rpjf = pwd_json_object["user_id"]
        my_pass_rpjf = pwd_json_object["my_pass"]
        target_db_rpjf = pwd_json_object["working_db"]
        return db_string_rpjf, user_id_rpjf, my_pass_rpjf, target_db_rpjf


def input_login_elements(in_file_ile):
    """
    This inputs inputs the login elements and returns the json.
    :return:  a placeholder boolean
    """
    db_string_ile = input("Enter full db string:")
    user_id_ile = input("Enter username:")
    my_pass_ile = input("password:")
    working_db = input("working db:")
    pw_df_creator_dict = {'db_string': db_string_ile, 'user_id': user_id_ile, 'my_pass': my_pass_ile,
                          'working_db': working_db}
    with open(in_file_ile, 'w', encoding='utf-8') as in_file_ile_file_object:
        json.dump(pw_df_creator_dict, in_file_ile_file_object)
    return True


# This is the main function
def awlc(pw_file_name_ls, askok=False):
    """

    :param pw_file_name_ls: a filename for the text login
    :return: tuple of db, username and id.
    """
    # determine if the login info exists
    login_info_exists = pathlib.Path(pw_file_name_ls).exists()
    # if it doesn't exist, call the login elements command to create the file.
    # then re-read it and return the contents.
    # I have it done this way to ensure that the file exists, all info is returned from
    # the main function here. If something happens it will throw an error we can trap.
    if not login_info_exists:
        print("Currently No Valid Login Data")
        input_login_elements(pw_file_name_ls)
        return read_password_json_file(pw_file_name_ls)
    else:
        # if it does exist
        # Check that the info is what the user wants
        # if not then call the login elements function again
        # and return the reading of the results as the tope
        if askok:
            print("Current user information exists")
            db_string, user_id, my_pass, working_db = read_password_json_file(pw_file_name_ls)
            my_pass_masked = len(my_pass) * '*'
            print(f'db_string:{db_string} \tYour User ID is {user_id} \tYour password is',
                f'{my_pass_masked}\t Target Database is {working_db}')

        if askok:
            proceed = input("Go forward with this information [Y/N]:").lower()
            if proceed == "y":
                return (db_string, user_id, my_pass, working_db)
            else:
                input_login_elements(pw_file_name_ls)
                return read_password_json_file(pw_file_name_ls)
        else:
            return read_password_json_file(pw_file_name_ls)
