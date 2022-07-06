'''
classes to manage the user status messages
'''
# pylint: disable=R0903
# pylint: disable=E0401
# pylint: disable=R0801 # Similar lines in 2 files
# pylint: disable=C0412 # Imports from package pymongo are not grouped (ungrouped-imports)


from loguru import logger
from pymongo import MongoClient
import pandas as pd
from pymongo.errors import DuplicateKeyError, BulkWriteError


def start_mongo():
    """
    start up a connection to MongoDB

    :return: a pymongo client object, and a database object
    """
    client = MongoClient(host='localhost', port=27017)

    return client


class StatusUpdates():
    """
    Collection of UserStatus messages
    """
    def __init__(self, database):
        """
        Initialize a collection called UserAccounts
        """
        self.database = database
        self.status_collection = database['status_collection']
        self.user_collection = database['user_collection']
        logger.info("New user status collection instance created")

    def __len__(self):
        """
        Number of user accounts in the collection
        """
        return self.status_collection.count_documents({})

    def add_status_in_chunks(self, filename, size=10000):
        '''
        Imports CSV file in chunks of a defined size
        '''
        chunk_number = 0
        for chunk in pd.read_csv(filename, chunksize=size, iterator=True):
            # print(f"CHUNK {chunk_number}")
            # Create an empty list and append chunk of status dicts to it
            status_chunk = []
            for index, row in chunk.iterrows():
                status = {}
                status['_id'] = row['STATUS_ID']
                status['user_id'] = row['USER_ID']
                status['status_text'] = row['STATUS_TEXT']
                status_chunk.append(status)
            try:
                # Use insert_many to import chunk into user_collection
                self.status_collection.insert_many(status_chunk)
            except BulkWriteError:
                return False
            chunk_number += 1
        return True

    def add_status(self, status_id, user_id, status_text):
        '''
        add a new status message to the collection
        '''
        result = self.user_collection.find_one({'_id': user_id})
        if result:
            new_status = {'_id': status_id,
                          'user_id': user_id,
                          'status_text': status_text}
            try:
                self.status_collection.insert_one(new_status)
            except DuplicateKeyError:
                print(f"Status ID {status_id} already exists")
                logger.error("ERROR: Status ID exists. Failed to add new status")
                return False
            logger.info("New status added successfully!")
            return True
        logger.error(
            "ERROR: User ID does not exist. Failed to add new status "
            "for non-existing user ID")
        return False

    def modify_status(self, status_id, user_id, status_text):
        '''
        Modifies a status message

        The new user_id and status_text are assigned to the existing message
        '''
        mongo_query = {'_id': status_id}
        if self.status_collection.count_documents(mongo_query) > 0:
            update_status = {'user_id': user_id,
                             'status_text': status_text}
            new_values = {"$set": update_status}
            self.status_collection.update_one(mongo_query, new_values)
            logger.info("Modified user status successfully!")
            return True
        logger.error("ERROR: User status ID doesn't exist. Failed to "
                     "modify user status")
        return False

    def delete_status(self, status_id):
        '''
        deletes the status message with id, status_id
        '''
        mongo_query = {'_id': status_id}
        result = self.status_collection.find_one({'_id': status_id})
        if result is None:
            logger.error("ERROR: Status ID doesn't exist. Failed to delete "
                         "status")
            return False
        self.status_collection.delete_one(mongo_query)
        logger.info(f"User status for {status_id} was deleted")
        return True

    def search_status(self, status_id):
        '''
        Find and return a status message by its status_id

        Returns an empty UserStatus object if status_id does not exist
        '''
        status = self.status_collection.find_one({'_id': status_id})
        if status is None:
            print(f"Status ID {status_id} was not found")
            logger.error(f"ERROR: Status ID {status_id} does not exist.")
            return None
        logger.info(f"Status ID {status_id} was found.")
        return status
