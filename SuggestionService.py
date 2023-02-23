# Oregon State University
# Author: James Lee
# Course: CS 361 Winter 2023
# Assignment: Course Project
# Description: controller for database. Executes queries

# REFERENCES:
#   https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html
#   https://docs.python.org/3/library/
#   https://pypi.org/project/python-dotenv/
#   https://docs.python.org/3/howto/sockets.html#creating-a-socket

# Imports
import socket
import os
import json
from mysql import connector
from mysql.connector import cursor as mysqlcursor
from dotenv import load_dotenv
load_dotenv('./.env')   # load files from ./.env

# ################
# Global variables
# ################

MYSQL_CONN = connector.MySQLConnection()
CURSOR = mysqlcursor.MySQLCursor()
EXIT_KEYWORD = 'stop'
_DBHOST = os.getenv('DBHOST')
_DBUSER = os.getenv('DBUSER')
_DBPW = os.getenv('DBPW')
_DB = os.getenv('DB')
_SERV_HOST = os.getenv('SERV_HOST')
_SERV_PORT = int(os.getenv('SERV_PORT'))
# Set DEBUG_ON to True to toggle print statements
DEBUG_ON = True


# #######################
# SOCKET CONNECTION SETUP
# #######################

def start_service() -> None:
    '''
    Connects mysql db and serves functions via socket connection
    '''
    init_mysql_conn()
    print("Starting service...")
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((_SERV_HOST, _SERV_PORT))
    print('Service started on socket %s:%s' % (_SERV_HOST, _SERV_PORT))

    recv_msg = ''        # message received over socket conn
    bufsize = 2048  # receive buffer size
    # main loop
    while True:
        if recv_msg == EXIT_KEYWORD:
            sock.close()
            close_mysql_conn()
            break
        sock.listen(1)  # listen for 1 connection
        serv_conn, addr = sock.accept()
        data = serv_conn.recv(bufsize)
        if data:
            recv_msg = data.decode()
            print("RECEIVED: %s" % recv_msg)
            res_msg = get_response(recv_msg)
            print("RESPONSE: %s" % res_msg.decode())
            serv_conn.sendall(res_msg)
        serv_conn.close()


def get_response(recv_msg: str) -> bytearray:
    '''
    Run appropriate query based on received message
    '''
    try:
        msg = recv_msg.split(',')
        print(f"msg list: {msg}")
        match msg[0]:
            case "create_suggestion":
                if len(msg) > 2:
                    suggestion = ','.join(msg[1:])
                elif len(msg) > 1:
                    suggestion = msg[1]
                else:
                    return b'Usage: create_suggestion,<suggestion>'

                print(suggestion)
                status = create_suggestion(suggestion)
                return status.encode()

            case "get_suggestion_table":
                table = get_suggestion_table()
                return json.dumps(table).encode()

            case "get_suggestion":
                if len(msg) > 1:
                    i = msg[1]
                    data = get_suggestion(int(i))
                    return json.dumps(data).encode()

            case "get_suggestion_id":
                if len(msg) > 2:
                    suggestion = ','.join(msg[1:])
                elif len(msg) > 1:
                    suggestion = msg[1]
                else:
                    return b'Usage: get_suggestion_id,<suggestion>'

                status = get_suggestion_id(suggestion)
                if status is not None:
                    return status.encode()
                else:
                    return b'No matching id'

            case "get_suggestion_id_range":
                id_range = get_suggestion_id_range()
                return json.dumps(id_range).encode()

            case "update_suggestion":
                if len(msg) > 3:
                    i = msg[1]
                    suggestion = ','.join(msg[2:])
                    status = update_suggestion(i, suggestion)
                    return status.encode()
                elif len(msg) > 2:
                    i = msg[1]
                    suggestion = msg[2]
                    status = update_suggestion(i, suggestion)
                    return status.encode()
                else:
                    return b'Usage: update_suggestion,<id>,<suggestion>'

            case "delete_suggestion":
                if len(msg) > 1:
                    i = int(msg[1])
                    status = delete_suggestion(i)
                    return status.encode()
                else:
                    return b'Usage: delete_suggestion,<id>'

            case 'stop':
                return b'Shutting down server'

            case _:     # default case
                return b'Invalid Request'
    except Exception as e:
        print(e)
        return b'Service error: %s' % e


# ######################
# MYSQL CONNECTION SETUP
# ######################

# initialize mysql connection
def init_mysql_conn(HOST: str = None,
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
    global MYSQL_CONN, CURSOR

    try:
        if type(HOST) is not str or\
                type(USER) is not str or\
                type(PW) is not str or\
                type(DB) is not str:
            print("initializing with .env variables") if DEBUG_ON else 0
            HOST = _DBHOST
            USER = _DBUSER
            PW = _DBPW
            DB = _DB
        if MYSQL_CONN.is_connected():
            close_mysql_conn()
            print('reconnecting...') if DEBUG_ON else 0
        MYSQL_CONN = connector.connect(host=HOST,
                                       user=USER,
                                       password=PW,
                                       database=DB,
                                       autocommit=True)

        CURSOR = MYSQL_CONN.cursor()

        print("Successfully connected to mysql server: %s@%s"
              % (USER, HOST)) if DEBUG_ON else 0
        print("Using database: %s" % DB) if DEBUG_ON else 0
    except Exception as err:
        print("Please verify mysql database connection settings and try again")
        CURSOR.close()
        MYSQL_CONN.close()
        raise err


# close connector and cursor
def close_mysql_conn() -> None:
    '''
    Close db connector and cursor.
    Return status of closing
    '''
    global MYSQL_CONN, CURSOR
    try:
        CURSOR.close()
        MYSQL_CONN.close()
        print("Connections closed succesfully.") if DEBUG_ON else 0
    except Exception as err:
        print('Error closing connection:\n', err)
        return False


def change_db(DB: str) -> None:
    '''
    change database if server connected
    '''
    global MYSQL_CONN, CURSOR
    if type(DB) is not str:
        print(str(TypeError("DB arg requires str type")))
        print("Using database: %s" % MYSQL_CONN.database)
    elif MYSQL_CONN.is_connected():
        try:
            MYSQL_CONN.close()
            MYSQL_CONN.connect(database=DB)
            CURSOR.close()
            CURSOR = MYSQL_CONN.cursor()
            print("Using database: %s" % DB) if DEBUG_ON else 0
        except Exception as e:
            print("Failed to connect to database.")
            print(e)
            init_mysql_conn()
    else:
        print("not connected to a server...")


# ###############
# CONTROL QUERIES
# ###############

# #################################
# Suggestions TABLE CRUD OPERATIONS
# #################################


# CREATE
def create_suggestion(suggestion: str) -> str:
    '''
    Creates a new entry in database with given suggestion.
    Max suggestion length is 500 characters.
    Returns status of query execution
    '''
    try:
        if len(suggestion) > 500:
            print('Suggestion exceeds character limit')
            return 'Suggestion exceeds character limit'
        qry = f"INSERT INTO Suggestions (suggestion) VALUES ('{suggestion}');"
        CURSOR.execute(qry)
        return 'Successfuly created suggestion'
    except Exception as e:
        print(e)
        return 'Error: %s' % e


# READ
def get_suggestion_table() -> list:
    '''
    Returns list of tuples from suggestion table:
    [(suggestionID, suggestion)]
    '''
    try:
        qry = "SELECT * FROM Suggestions"
        CURSOR.execute(qry)
        table = CURSOR.fetchall()
        return table
    except Exception as e:
        CURSOR.reset()
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

        CURSOR.execute(qry)
        suggestion = tuple(s[0] for s in CURSOR)
        return suggestion
    except Exception as e:
        CURSOR.reset()
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
        CURSOR.execute(qry)
        i = CURSOR.fetchall()
        i = i[0][0] if len(i) > 0 else None
        return i
    except Exception as e:
        CURSOR.reset()
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
        CURSOR.execute(qry_min)
        min_i = CURSOR.fetchall()[0][0]
        CURSOR.execute(qry_max)
        max_i = CURSOR.fetchall()[0][0]
        return min_i, max_i
    except Exception as e:
        CURSOR.reset()
        print(e)
        return None


# UPDATE
def update_suggestion(suggestion_id: int, suggestion: str) -> str:
    '''
    Update suggestion with given suggestion id.
    Returns status as a string
    '''
    try:
        qry = '''
        UPDATE Suggestions
        SET suggestion = '%s'
        WHERE suggestionID = %i
        ''' % (suggestion, suggestion_id)
        CURSOR.execute(qry)
        return "Executed update query"
    except Exception as e:
        CURSOR.reset()
        print(e)
        return "Error: %s" % e


# DELETE
def delete_suggestion(suggestion_id: int) -> str:
    '''
    Delete suggestion with given suggestion id.
    WARNING: This creates gaps in database id list
    Returns status as a string
    '''
    try:
        qry = '''
        DELETE FROM Suggestions
        WHERE suggestionID = %i;
        ''' % suggestion_id
        CURSOR.execute(qry)
        return "Executed delete query"
    except Exception as e:
        print(e)
        return "Error: %s" % e


if __name__ == '__main__':
    start_service()
