import socket
import json
import random
import os
from dotenv import load_dotenv
load_dotenv('./.env')

_HOST = os.getenv('SERV_HOST')
_PORT = int(os.getenv('SERV_PORT'))


def send_req(req: str) -> str:
    '''
    Send request to service.
    returns response from service.
    '''
    sock = socket.socket()
    sock.connect((_HOST, _PORT))
    print("Sending request: %s" % req)
    sock.sendall(req.encode())
    res = ''
    while True:
        data = sock.recv(2048)
        if not data:
            break
        res += data.decode()
    sock.close()
    return res


def create_suggestion(sugg: str) -> str:
    req = 'create_suggestion,%s' % sugg
    status = send_req(req)
    return status


def get_suggestion_table() -> list:
    req = 'get_suggestion_table'
    res = send_req(req)
    table = json.loads(res)
    return table


def get_suggestion(i: int = None) -> tuple:
    req = 'get_suggestion'
    if i is not None:
        req += ',%i' % i
    res = send_req(req)
    suggestions = tuple(json.loads(res))
    return suggestions


def get_suggestion_id(sugg: str) -> int:
    req = 'get_suggestion_id,%s' % sugg
    res = send_req(req)
    try:
        i = int(res)
        return i
    except Exception:
        print(res)
        return None


def get_suggestion_id_range() -> tuple[int, int]:
    req = 'get_suggestion_id_range'
    res = send_req(req)
    s_range = tuple(json.loads(res))
    return s_range


def update_suggestion(i: int, sugg: str) -> str:
    req = 'update_suggestion,%i,%s' % (i, sugg)
    status = send_req(req)
    return status


def delete_suggestion(i: int) -> str:
    # WARNING THIS WILL CREATE HOLES IN DB ID LIST
    req = 'delete_suggestion'
    status = send_req(req)
    return status


def get_random_suggestion() -> str:
    '''
    Use connector to get a random suggestion.
    '''
    req = 'get_suggestion_id_range'
    res = send_req(req)
    suggestion_range = json.loads(res)
    print('min id: %i' % suggestion_range[0])
    print('max id: %i' % suggestion_range[1])
    suggestion = None
    while suggestion is None:
        i = random.randint(suggestion_range[0], suggestion_range[1])
        req = 'get_suggestion,%i' % i
        res = send_req(req)
        if len(res) > 0:
            res_list = json.loads(res)
            suggestion = res_list[0]
    return suggestion


if __name__ == '__main__':
    suggestion = get_random_suggestion()
    print('Random Suggestion:', suggestion)
    table = get_suggestion_table()
    print("Printing table...")
    for row in table:
        print(row)
    status = create_suggestion('Make this suggestion now!!')
    print(status)
    table = get_suggestion_table()
    print("Printing table...")
    for row in table:
        print(row)
    status = send_req('stop')
    print(status)
