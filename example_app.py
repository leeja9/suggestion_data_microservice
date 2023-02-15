import SuggestionService as connector
import random


def get_random_suggestions(conn: connector) -> tuple[str]:
    '''
    Use connector to get a random suggestion.
    '''
    min_i, max_i = connector.get_suggestion_id_range()
    i = random.randint(min_i, max_i)
    suggestion = connector.get_suggestion(i)
    return suggestion


if __name__ == '__main__':
    # Initiate connection to database
    connector.init_connection()

    # Example of function taking connector as argument
    suggestions = get_random_suggestions(connector)
    for suggestion in suggestions:
        print('\nSuggestion:\n' + suggestion + '\n')

    # Example of grabbing table directly from connector
    table = connector.get_suggestion_table()
    print('Suggestion Table data:\n', table)

    # Example of updating data
    connector.update_suggestion(1, 'Example update')
    table = connector.get_suggestion_table()
    print('Suggestion Table with updated data:\n', table)

    connector.cursor.execute('SELECT * FROM Suggestions')
    table = connector.cursor.fetchall()
    print('Table data directly from mysql cursor:\n', table)

    # Example of deleting a row from the Suggestions table
    connector.delete_suggestion(1)
    table = connector.get_suggestion_table()
    print('Table data after deleting a row:\n', table)

    # Close connection to database
    connector.close_connection()
