'''
main driver for a simple social network project
'''
# pylint: disable=W0612
# pylint: disable=E0401

import os
import csv
import multiprocessing
import pandas as pd
from loguru import logger
from pymongo import MongoClient
import users
import user_status


class MongoDBConnection():
    """Mongo DB Connection"""

    def __init__(self, host='127.0.0.1', port=27017):
        self.host = host
        self.port = port
        self.connection = MongoClient(self.host, self.port)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def close(self):
        """
        close client connection
        """
        self.connection.close()


def init_user_collection(database):
    '''
    Creates and returns a new instance of UserCollection
    '''
    return users.UserAccounts(database)


def init_status_collection(database):
    '''
    Creates and returns a new instance of UserStatusCollection
    '''
    return user_status.StatusUpdates(database)


def load_users(filename, user_collection):
    '''
    Opens a CSV file with user data and
    adds it to an existing instance of
    UserCollection

    Requirements:
    - If a user_id already exists, it
    will ignore it and continue to the
    next.
    - Returns False if there are any errors
    (such as empty fields in the source CSV file)
    - Otherwise, it returns True.
    '''
    file_exist = os.path.exists(filename)
    if file_exist is False:
        logger.error("Sorry, this file doesn't exist.")
        return False
    with open(filename, 'r', encoding='utf-8') as file:
        user_data = csv.DictReader(file, delimiter=',')
        for row in user_data:
            user_collection.add_user(row['USER_ID'],
                                     row['EMAIL'],
                                     row['NAME'],
                                     row['LASTNAME'])
    return True


def load_status_updates(filename, status_collection):
    '''
    Opens a CSV file with status data and adds it to an existing
    instance of UserStatusCollection

    Requirements:
    - If a status_id already exists, it will ignore it and continue to
      the next.
    - Returns False if there are any errors(such as empty fields in the
      source CSV file)
    - Otherwise, it returns True.
    '''
    file_exist = os.path.exists(filename)
    if file_exist is False:
        logger.error("Sorry, this file doesn't exist.")
        return False
    with open(filename, 'r', encoding='utf-8') as file:
        status_data = csv.DictReader(file, delimiter=',')
        for row in status_data:
            status_collection.add_status(row['STATUS_ID'],
                                         row['USER_ID'],
                                         row['STATUS_TEXT'])
    logger.info("Loaded user status database from file successfully!")
    return True


def load_users_chunks(filename, user_collection):
    '''
    Opens a CSV file with user data and
    adds it to an existing instance of
    UserCollection

    Load users data in chunks
    '''
    file_exist = os.path.exists(filename)
    if file_exist is False:
        logger.error("Sorry, this file doesn't exist.")
        return False
    user_collection.add_user_in_chunks(filename)
    return True


def load_users_multiprocess(filename, size=100):
    '''
    Imports CSV file in chunks of a defined size
    '''
    chunk_number = 0
    processes = []
    for chunk in pd.read_csv(filename, chunksize=size, iterator=True):
        print(f"CHUNK {chunk_number}")
        # Create an empty list and append chunk of user dicts to it
        # user_chunk = []
        # for index, row in chunk.iterrows():
        #     user = {}
        #     user['_id'] = row['USER_ID']
        #     user['email'] = row['EMAIL']
        #     user['name'] = row['NAME']
        #     user['last_name'] = row['LASTNAME']
        #     user_chunk.append(user)
        proc = multiprocessing.Process(target=load_users_worker,
                                       args=(chunk, ))
        processes.append(proc)
        proc.start()
        chunk_number += 1
    for proc in processes:
        print(f"waiting on {proc.name}")
        proc.join()
    return True


def load_users_worker(chunk):
    """
    The worker function for load users with multiprocessing
    """
    with MongoDBConnection() as mongo:
        database = mongo.connection.SocialNetwork
        user_collection = init_user_collection(database)
        for _, row in chunk.iterrows():
            user_collection.add_user(row['USER_ID'],row['EMAIL'],row['NAME'],row['LASTNAME'])
        # user_collection.add_users(chunk)
    return True


def load_status_updates_chunks(filename, status_collection):
    '''
    Opens a CSV file with status data and adds it to an existing
    instance of UserStatusCollection

    Load status updates in chunks
    '''
    file_exist = os.path.exists(filename)
    if file_exist is False:
        logger.error("Sorry, this file doesn't exist.")
        return False
    status_collection.add_status_in_chunks(filename)
    return True


def load_status_multiprocess(filename, size=1000):
    '''
    Imports CSV file in chunks of a defined size
    '''
    chunk_number = 0
    processes = []
    for chunk in pd.read_csv(filename, chunksize=size, iterator=True):
        print(f"CHUNK {chunk_number}")
        proc = multiprocessing.Process(target=load_status_worker,
                                       args=(chunk, ))
        processes.append(proc)
        proc.start()
        chunk_number += 1
    for proc in processes:
        print(f"waiting on {proc.name}")
        proc.join()
    return True


def load_status_worker(chunk):
    """
    The worker function for load status updates with multiprocessing
    """
    with MongoDBConnection() as mongo:
        database = mongo.connection.SocialNetwork
        user_collection = init_user_collection(database)
        status_collection = init_status_collection(database)
        for _, row in chunk.iterrows():
            status_collection.add_status(row['STATUS_ID'],row['USER_ID'],row['STATUS_TEXT'])
    return True


def add_user(user_id, email, user_name, user_last_name, user_collection):
    '''
    Creates a new instance of User and stores it in user_collection
    (which is an instance of UserCollection)

    Requirements:
    - user_id cannot already exist in user_collection.
    - Returns False if there are any errors (for example, if
      user_collection.add_user() returns False).
    - Otherwise, it returns True.
    '''
    return user_collection.add_user(user_id,email,user_name,user_last_name)


def add_users(user_chunk, user_collection):
    """
    Add users to database in chunk
    """
    return user_collection.add_users(user_chunk)


def update_user(user_id, email, user_name, user_last_name, user_collection):
    '''
    Updates the values of an existing user

    Requirements:
    - Returns False if there any errors.
    - Otherwise, it returns True.
    '''
    return user_collection.modify_user(user_id, email, user_name, user_last_name)


def delete_user(user_id, user_collection):
    '''
    Deletes a user from user_collection.

    Requirements:
    - Returns False if there are any errors (such as user_id not found)
    - Otherwise, it returns True.
    '''
    return user_collection.delete_user(user_id)


def search_user(user_id, user_collection):
    '''
    Searches for a user in user_collection(which is an instance of
    UserCollection).

    Requirements:
    - If the user is found, returns the corresponding User instance.
    - Otherwise, it returns None.
    '''
    return user_collection.search_user(user_id)


def add_status(user_id, status_id, status_text, status_collection):
    '''
    Creates a new instance of UserStatus and stores it in
    user_collection(which is an instance of UserStatusCollection)

    Requirements:
    - status_id cannot already exist in user_collection.
    - Returns False if there are any errors (for example, if
      user_collection.add_status() returns False).
    - Otherwise, it returns True.
    '''
    return status_collection.add_status(status_id, user_id, status_text)


def update_status(status_id, user_id, status_text, status_collection):
    '''
    Updates the values of an existing status_id

    Requirements:
    - Returns False if there any errors.
    - Otherwise, it returns True.
    '''
    return status_collection.modify_status(status_id, user_id, status_text)


def delete_status(status_id, status_collection):
    '''
    Deletes a status_id from user_collection.

    Requirements:
    - Returns False if there are any errors (such as status_id not found)
    - Otherwise, it returns True.
    '''
    return status_collection.delete_status(status_id)


def search_status(status_id, status_collection):
    '''
    Searches for a status in status_collection

    Requirements:
    - If the status is found, returns the corresponding
    UserStatus instance.
    - Otherwise, it returns None.
    '''
    return status_collection.search_status(status_id)
