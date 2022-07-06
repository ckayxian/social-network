'''
Provides a basic frontend
'''
# pylint: disable=C0103
# pylint: disable=E0401
# pylint: disable=W0612 # Unused variable 't' (unused-variable) # context manager

import datetime
import sys
from loguru import logger
import main
from timer_context import Timer

#log code
#create a file called log_mm_dd_yyyy.log
date_time = datetime.datetime.today()
log_date = date_time.strftime("%d_%m_%Y")
logger.remove()
logger.add(f"log_{log_date}.log")


def load_users():
    '''
    Loads user accounts from a file
    '''
    filename = input('Enter filename of user file: ')
    with Timer() as t:
        result = main.load_users(filename, user_collection)
    if not result:
        logger.error("Error occurred when trying to load user accounts "
                     "database from file.")
    else:
        logger.info("User database loaded from file successfully!")


def load_users_chunks():
    """
    Load users from file in chunks
    """
    filename = input('Enter filename of user file: ')
    with Timer() as t:
        result = main.load_users_chunks(filename, user_collection)
    if not result:
        logger.error("Error occurred when trying to load user accounts "
                     "database from file.")
    else:
        logger.info("User database loaded from file successfully!")


def load_users_multiprocess():
    '''
    Loads user accounts from a file
    '''
    filename = input('Enter filename of user file: ')
    with Timer() as t:
        result = main.load_users_multiprocess(filename, size=500)
    if not result:
        logger.error("Error occurred when trying to load user accounts "
                     "database from file.")
    else:
        logger.info("User database loaded from file successfully!")


def load_status_updates():
    '''
    Loads status updates from a file
    '''
    filename = input('Enter filename for status file: ')
    with Timer() as t:
        result = main.load_status_updates(filename, status_collection)
    if not result:
        logger.error("Error occurred when trying to load status updates "
                     "database from file")
    else:
        logger.info("User status loaded from file successfully!")


def load_status_updates_chunks():
    """
    Load status updates in chunks
    """
    filename = input('Enter filename for status file: ')
    with Timer() as t:
        result = main.load_status_updates_chunks(filename, status_collection)
    if not result:
        logger.error("Error occurred when trying to load status updates "
                     "database from file")
    else:
        logger.info("User status loaded from file successfully!")


def load_status_updates_multiprocess():
    """
    Load status updates in chunks
    """
    filename = input('Enter filename for status file: ')
    with Timer() as t:
        result = main.load_status_multiprocess(filename, size=50000)
    if not result:
        logger.error("Error occurred when trying to load status updates "
                     "database from file")
    else:
        logger.info("User status loaded from file successfully!")


def add_user():
    '''
    Adds a new user into the database
    '''
    user_id = input('User ID: ')
    email = input('User email: ')
    user_name = input('User name: ')
    user_last_name = input('User last name: ')
    with Timer() as t:
        result = main.add_user(user_id,
                               email,
                               user_name,
                               user_last_name,
                               user_collection)
    if not result:
        logger.error("An error occurred while trying to add new user")
    else:
        logger.info("User was successfully added")


# pylint: disable=E1120
def update_user():
    '''
    Updates information for an existing user
    '''
    user_id = input('User ID: ')
    email = input('User email: ')
    user_name = input('User name: ')
    user_last_name = input('User last name: ')
    with Timer() as t:
        result = main.update_user(user_id,
                                  email,
                                  user_name,
                                  user_last_name,
                                  user_collection)
    if not result:
        logger.error("An error occurred while trying to update user")
    else:
        logger.info("User was successfully updated")


def search_user():
    '''
    Searches a user in the database
    '''
    user_id = input('Enter user ID to search: ')
    with Timer() as t:
        result = main.search_user(user_id, user_collection)
    if result is None:
        print("ERROR: User does not exist")
        logger.error("An error occurred while trying to search user")
    else:
        print(f"User ID: {result['_id']}")
        print(f"Email: {result['email']}")
        print(f"Name: {result['name']}")
        print(f"Last name: {result['last_name']}")
        logger.info(f"User {user_id} was found")


def delete_user():
    '''
    Deletes user from the database
    '''
    user_id = input('User ID: ')
    with Timer() as t:
        result = main.delete_user(user_id, user_collection)
    if not result:
        logger.error("An error occurred while trying to delete user")
    else:
        logger.info("User was successfully deleted")


def add_status():
    '''
    Adds a new status into the database
    '''
    user_id = input('User ID: ')
    status_id = input('Status ID: ')
    status_text = input('Status text: ')
    with Timer() as t:
        result = main.add_status(user_id,
                                 status_id,
                                 status_text,
                                 status_collection)
    if not result:
        logger.error("An error occurred while trying to add new status")
    else:
        logger.info("New status was successfully added")


def update_status():
    '''
    Updates information for an existing status
    '''
    user_id = input('User ID: ')
    status_id = input('Status ID: ')
    status_text = input('Status text: ')
    with Timer() as t:
        result = main.update_status(status_id,
                                    user_id,
                                    status_text,
                                    status_collection)
    if not result:
        logger.error("An error occurred while trying to update status")
        return False
    logger.info("Status was successfully updated")
    return True


def search_status():
    '''
    Searches a status in the database
    '''
    status_id = input('Enter status ID to search: ')
    with Timer() as t:
        result = main.search_status(status_id, status_collection)
    if result is None:
        logger.error("ERROR: Status does not exist")
    else:
        print(f"User ID: {result['user_id']}")
        print(f"Status ID: {result['_id']}")
        print(f"Status text: {result['status_text']}")
        logger.info("Status was found.")


def delete_status():
    '''
    Deletes status from the database
    '''
    status_id = input('Status ID: ')
    with Timer() as t:
        result = main.delete_status(status_id, status_collection)
    if not result:
        logger.error("An error occurred while trying to delete status")
    else:
        logger.info("Status was successfully deleted")


def clear_database():
    """
    Clear existing database
    """
    permission = input("Are you sure to clear your existing database? (Y/N)")
    if permission.lower() == 'y':
        mongo.connection.drop_database('SocialNetwork')
    elif permission.lower() == 'n':
        print("The existing database was not cleared.")
    else:
        print("Not valid option")


def quit_program():
    '''
    Quits program
    '''
    sys.exit()


if __name__ == '__main__':
    with main.MongoDBConnection() as mongo:
        database = mongo.connection.SocialNetwork
        user_collection = main.init_user_collection(database)
        status_collection = main.init_status_collection(database)

        menu_options = {
            'A': load_users,
            'B': load_status_updates,
            'C': add_user,
            'D': update_user,
            'E': search_user,
            'F': delete_user,
            'G': add_status,
            'H': update_status,
            'I': search_status,
            'J': delete_status,
            'K': clear_database,
            'L': load_users_chunks,
            'M': load_status_updates_chunks,
            'N': load_users_multiprocess,
            'O': load_status_updates_multiprocess,
            'Q': quit_program
        }
        while True:
            user_selection = input("""
                                A: Load user database
                                B: Load status database
                                C: Add user
                                D: Update user
                                E: Search user
                                F: Delete user
                                G: Add status
                                H: Update status
                                I: Search status
                                J: Delete status
                                K: Clear database
                                L: Load users chunks
                                M: Load status updates chunks
                                N: Load users multiprocess
                                O: Load status updates multiprocess
                                Q: Quit
    
                                Please enter your choice: """)
            if user_selection.upper() in menu_options:
                menu_options[user_selection.upper()]()
            else:
                print("Invalid option")
