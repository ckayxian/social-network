'''
Classes for user information for the social network project
'''
# pylint: disable=R0903
# pylint: disable=E0401
# pylint: disable=R0801 # Similar lines in 2 files


import pandas as pd
from loguru import logger
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, BulkWriteError


def start_mongo():
    """
    start up a connection to MongoDB

    :return: a pymongo client object, and a database object
    """
    client = MongoClient(host='localhost', port=27017)

    return client


class UserAccounts():
    """
    a collection to hold user accounts
    """
    def __init__(self,database):
        """
        Initialize a collection called UserAccounts
        """
        self.database = database
        self.user_collection = database['user_collection']
        self.status_collection = database['status_collection']

    def __len__(self):
        """
        Number of user accounts in the collection
        """
        return self.user_collection.count_documents({})

    def add_users(self, user_chunk):
        """
        Insert a chunk of users to database
        """
        try:
            self.user_collection.insert_many(user_chunk, ordered=False)
            return True
        except BulkWriteError as err:
            details = err.details
            for error in details['writeErrors']:
                logger.error(f"user_id: {error['keyValue']['_id']} failed to add")
            return False

    def add_user_in_chunks(self, filename, size=100):
        '''
        Imports CSV file in chunks of a defined size
        '''
        chunk_number = 0
        for chunk in pd.read_csv(filename, chunksize=size, iterator=True):
            # print(f"CHUNK {chunk_number}")
            # Create an empty list and append chunk of user dicts to it
            user_chunk = []
            for index, row in chunk.iterrows():
                user = {}
                user['_id'] = row['USER_ID']
                user['email'] = row['EMAIL']
                user['name'] = row['NAME']
                user['last_name'] = row['LASTNAME']
                user_chunk.append(user)
            try:
                # Use insert_many to import chunk into user_collection
                self.user_collection.insert_many(user_chunk, ordered=False)
            except BulkWriteError as err:
                details = err.details
                for error in details['writeErrors']:
                    logger.error(
                        f"user_id: {error['keyValue']['_id']} failed to add")
                return False
            chunk_number += 1
        return True

    def add_user(self, user_id, email, user_name, user_last_name):
        '''
        Adds a new user to the collection
        '''
        new_user = {'_id': user_id,
                    'email': email,
                    'name': user_name,
                    'last_name': user_last_name
                    }
        try:
            self.user_collection.insert_one(new_user)
        except DuplicateKeyError:
            print(f"User ID {user_id} already exists")
            logger.error(f"Failed to add an existing user {user_id}")
            return False
        return True

    def modify_user(self, user_id, email, user_name, user_last_name):
        '''
        Modifies an existing user
        '''
        mongo_query = {'_id': user_id}
        if self.user_collection.count_documents(mongo_query) > 0:
            updated_user_data = {
                'email': email,
                'name': user_name,
                'last_name': user_last_name
            }
            new_values = {"$set": updated_user_data}
            self.user_collection.update_one(mongo_query, new_values)
            logger.info("User information updated successfully!")
            return True
        logger.error("User ID doesn't exist. Failed to update user.")
        return False

    def delete_user(self, user_id):
        '''
        Deletes an existing user
        '''
        mongo_query = {'_id': user_id}
        result = self.user_collection.find_one({'_id': user_id})
        if result:
            mongo_query_status = {'user_id': user_id}
            self.status_collection.delete_many(mongo_query_status)
            self.user_collection.delete_one(mongo_query)
            logger.info(f"User {user_id} was deleted.")
            return True
        logger.error("ERROR: User ID doesn't exist. Failed to delete user.")
        return False

    def search_user(self, user_id):
        '''
        Searches for user data
        '''
        user = self.user_collection.find_one({'_id': user_id})
        if user is None:
            logger.error("ERROR: User ID doesn't exist.")
            return None
        logger.info(f"User ID {user_id} was found.")
        return user
