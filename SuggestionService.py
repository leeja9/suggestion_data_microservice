# Oregon State University
# Author: James Lee
# Course: CS 361 Winter 2023
# Assignment: Course Project
# Description: controller for database. Executes queries

# REFERENCES:
#   https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html
#   https://docs.python.org/3/library/
#   https://pypi.org/project/python-dotenv/

# Imports
# import datetime
import os
from mysql import connector
from mysql.connector import cursor as mysqlcursor
from dotenv import load_dotenv


# ################
# Global variables
# ################

connection = connector.MySQLConnection()
cursor = mysqlcursor.MySQLCursor()
ENV_PATH = './.env'
# Set DEBUG_ON to True to toggle print statements
DEBUG_ON = False


# ################
# CONNECTION SETUP
# ################

# initialize connection
def init_connection(HOST: str = None,
                    USER: str = None,
                    PW: str = None,
                    DB: str = None) -> None:
    '''
    Initialize connection for query execution.
    Loads db setup variables from .env if none given.

    Input variables:
        HOST:   ip address of host
        USER:   mysql database username
        PW:     mysql database password
        DB:     database (schema) name
    '''
    global connection, cursor

    try:
        load_dotenv(ENV_PATH)   # load files from ./.env
        if type(HOST) is not str or\
                type(USER) is not str or\
                type(PW) is not str or\
                type(DB) is not str:
            print("initializing with .env variables") if DEBUG_ON else 0
            HOST = os.getenv('DBHOST')
            USER = os.getenv('DBUSER')
            PW = os.getenv('DBPW')
            DB = os.getenv('DB')
        if connection.is_connected():
            close_connection()
            print('reconnecting...') if DEBUG_ON else 0
        connection = connector.connect(host=HOST,
                                       user=USER,
                                       password=PW,
                                       database=DB,
                                       autocommit=True)

        cursor = connection.cursor()

        print("Successfully connected to mysql server: %s@%s"
              % (USER, HOST)) if DEBUG_ON else 0
        print("Using database: %s" % DB) if DEBUG_ON else 0
    except Exception as err:
        print("Please verify mysql database connection settings and try again")
        cursor.close()
        connection.close()
        raise err


# close connector and cursor
def close_connection() -> None:
    '''
    Close db connector and cursor.
    Return status of closing
    '''
    global connection, cursor
    try:
        cursor.close()
        connection.close()
        print("Connections closed succesfully.") if DEBUG_ON else 0
    except Exception as err:
        print('Error closing connection:\n', err)
        return False


def change_db(DB: str) -> None:
    '''
    change database if server connected
    '''
    global connection, cursor
    if type(DB) is not str:
        print(str(TypeError("DB arg requires str type")))
        print("Using database: %s" % connection.database)
    elif connection.is_connected():
        try:
            connection.close()
            connection.connect(database=DB)
            cursor.close()
            cursor = connection.cursor()
            print("Using database: %s" % DB) if DEBUG_ON else 0
        except Exception as e:
            print("Failed to connect to database.")
            print(e)
            init_connection()
    else:
        print("not connected to a server...")


# ###############
# CONTROL QUERIES
# ###############

# #################################
# Suggestions TABLE CRUD OPERATIONS
# #################################


# CREATE
def create_suggestion(suggestion: str) -> None:
    '''
    Creates a new entry in database with given suggestion.
    Max suggestion length is 500 characters.
    '''
    try:
        if len(suggestion) > 500:
            print('Suggestion exceeds character limit')
            return ''
        qry = "INSERT INTO Suggestions (suggestion) VALUES (%s);" % suggestion
        cursor.execute(qry)
    except Exception as e:
        print(e)
        return ''


# READ
def get_suggestion_table() -> list:
    '''
    Returns list of tuples from suggestion table:
    [(suggestionID, suggestion)]
    '''
    try:
        qry = "SELECT * FROM Suggestions"
        cursor.execute(qry)
        table = cursor.fetchall()
        return table
    except Exception as e:
        cursor.reset()
        print(e)
        return None


def get_suggestion(suggestion_id: int = None) -> tuple:
    '''
    Get a tuple of suggestions. If no id given, returns all suggestions.
    '''
    try:
        qry = "SELECT suggestion FROM Suggestions"
        if suggestion_id is not None:
            qry_filter = " WHERE suggestionID = %i" % suggestion_id
            qry = qry + qry_filter

        cursor.execute(qry)
        suggestion = tuple(s[0] for s in cursor)
        return suggestion
    except Exception as e:
        cursor.reset()
        print(e)
        return None


def get_suggestion_id(suggestion: str) -> int:
    '''
    Get the id of a suggestion if there's a match in db.
    Returns None if no match
    '''
    try:
        qry = '''
        SELECT suggestionID
        FROM Suggestions
        WHERE suggestion = '%s'
        ''' % suggestion
        cursor.execute(qry)
        i = cursor.fetchall()
        i = i[0][0] if len(i) > 0 else None
        return i
    except Exception as e:
        cursor.reset()
        print(e)
        return None


def get_suggestion_id_range() -> tuple[int, int]:
    '''
    Get the maximum and minimum integer values of Suggestions table.
    Returns [min, max] inclusive.
    Returns None if error.
    '''
    try:
        qry_min = "SELECT MIN(suggestionID) FROM Suggestions"
        qry_max = "SELECT MAX(suggestionID) FROM Suggestions"
        cursor.execute(qry_min)
        min_i = cursor.fetchall()[0][0]
        cursor.execute(qry_max)
        max_i = cursor.fetchall()[0][0]
        return min_i, max_i
    except Exception as e:
        cursor.reset()
        print(e)
        return None


# UPDATE
def update_suggestion(suggestion_id: int, suggestion: str) -> None:
    '''
    Update suggestion with given suggestion id.
    '''
    try:
        qry = '''
        UPDATE Suggestions
        SET suggestion = '%s'
        WHERE suggestionID = %i
        ''' % (suggestion, suggestion_id)
        cursor.execute(qry)
    except Exception as e:
        cursor.reset()
        print(e)


# DELETE
def delete_suggestion(suggestion_id: int) -> None:
    '''
    Delete suggestion with given suggestion id.
    WARNING: This creates gaps in database id list
    '''
    try:
        qry = '''
        DELETE FROM Suggestions
        WHERE suggestionID = %i;
        ''' % suggestion_id
        cursor.execute(qry)
    except Exception as e:
        print(e)
        return None


if __name__ == '__main__':
    init_connection()
    close_connection()
