import indeed_search_function
import aws_login_credentials

# Read the user and password file.
login_file='login_info.json'
# The log
# The login credentials tuple contains db_string, username, password
login_credentials_tuple = aws_login_credentials(login_file)


db_string = "postgres+psycopg2://lord_nic:@davidsstone.cv3nmvm2rhep.us-east-2.rds.amazonaws.com:5432/davids_stone"
