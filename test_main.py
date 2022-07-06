"""
tests for the main driver code of the Social Network
"""
# pylint: disable=C0103
# pylint: disable=W0107
# pylint: disable=C0301
# pylint: disable=R0201
# pylint: disable=W0621 # Redefining name 'empty_db' from outer scope

import pytest
import main
from users import UserAccounts
from user_status import StatusUpdates

CLIENT = main.MongoDBConnection().connection


@pytest.fixture
def empty_db():
    """
    provides an empty database for testing
    """
    CLIENT.drop_database('test_main')
    # create it again
    return CLIENT.test_main

@pytest.fixture
def full_db():
    """
    provides an full database for testing
    """
    CLIENT.drop_database('test_main')

    # create it again
    database = CLIENT.test_main
    user_coll = database.user_collection
    status_coll = database.status_collection

    # populate it
    user_coll.insert_one({'_id': 'ckayx15',
                          'email': 'ckayx15@uw.edu',
                          'name': 'Kay',
                          'last_name': 'Xian'})
    status_coll.insert_one({'_id': 'ckayx_00001',
                            'user_id': 'ckayx15',
                            'status_text': 'Test collection set up'})

    return database


def test_init_user_collection(empty_db):
    '''
    Creates and returns a new instance of UserCollection
    '''
    uc = main.init_user_collection(empty_db)

    assert isinstance(uc, UserAccounts)
    assert len(uc) == 0


def test_add_user(empty_db):
    '''
    Test the add_user method in users.UserCollection()
    '''
    uc = main.init_user_collection(empty_db)
    au = main.add_user('evmiles97','eve.miles@uw.edu','Eve','Miles', uc)
    au_exist = main.add_user('evmiles97','eve.miles@uw.edu','Eve','Miles', uc)

    assert au is True
    assert au_exist is False


def test_load_user(empty_db):
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
    uc = main.init_user_collection(empty_db)
    load = main.load_users('accounts.csv', uc)

    assert load is True


def test_load_wrong_file(empty_db):
    """
    Returns false if file does not exist
    """
    uc = main.init_user_collection(empty_db)
    load_wrong = main.load_users('wrong.csv', uc)

    assert load_wrong is False

def test_load_users_chunks(empty_db):
    """
    Returns True if load file successfully
    Returns False if file does not exist
    """
    uc = main.init_user_collection(empty_db)
    load_chunks = main.load_users_chunks('accounts.csv', uc)
    load_wrong = main.load_users_chunks('wrong.csv', uc)

    assert load_chunks is True
    assert load_wrong is False


def test_update_user(empty_db):
    '''
    Updates the values of an existing user

    Requirements:
    - Returns False if there any errors.
    - Otherwise, it returns True.
    '''
    uc = main.init_user_collection(empty_db)
    main.add_user('ckayx15', 'ckayx15@uw.edu', 'Kay', 'Xian', uc)
    update_exist_id = main.update_user('ckayx15', 'ckayx15@uw.edu', 'Shiqi', 'Xian', uc)

    assert update_exist_id is True

def test_update_user_non_exist(empty_db):
    '''
    Updates the values of an existing user

    Requirements:
    - Returns False if there any errors.
    - Otherwise, it returns True.

    Should return false for non-existing user id
    '''
    uc = main.init_user_collection(empty_db)
    main.add_user('ckayx15', 'ckayx15@uw.edu', 'Kay', 'Xian', uc)
    update_non_exist_id = main.update_user('ckayx159','ckayx15@uw.edu',
                                           'Kay', 'Xian', uc)

    assert update_non_exist_id is False


def test_delete_user(empty_db):
    """
    Test delete user calling from main.py
    """

    uc = main.init_user_collection(empty_db)
    main.add_user('ckayx15', 'ckayx15@uw.edu', 'Kay', 'Xian', uc)
    del_main_exist = main.delete_user('ckayx15', uc)
    del_main_non_exist = main.delete_user('bbq15', uc)

    assert del_main_exist is True
    assert del_main_non_exist is False


def test_main_search_user(empty_db):
    """
    Test search_user calling from main.py
    """
    uc = main.init_user_collection(empty_db)
    main.add_user('ckayx15', 'ckayx15@uw.edu', 'Kay', 'Xian', uc)
    search_exist = main.search_user('ckayx15', uc)
    search_non_exist = main.search_user('ckayx159', uc)

    assert search_exist['_id'] == 'ckayx15'
    assert search_non_exist is None


def test_init_status_collection(empty_db):
    '''
    Creates and returns a new instance of UserStatusCollection
    '''
    sc = main.init_status_collection(empty_db)

    assert isinstance(sc, StatusUpdates)
    assert len(sc) == 0

def test_load_status_updates(empty_db):
    '''
    Test load_status_updates
    '''
    sc = main.init_status_collection(empty_db)
    load_good_status = main.load_status_updates('status_updates.csv', sc)
    load_wrong_status = main.load_status_updates('wrong.csv', sc)

    assert load_good_status is True
    assert load_wrong_status is False


def test_main_add_status(empty_db):
    """
    Test add_status calling from main.py
    """
    uc = main.init_user_collection(empty_db)
    sc = main.init_status_collection(empty_db)
    main.add_user('dave03','david.yuen@gmail.com','David','Yuen', uc)
    new_status_main = main.add_status('dave03',
                                      'dave03_00001',
                                      "Sunny in Seattle this morning",sc)
    exist_status_main = main.add_status('dave03',
                                        'dave03_00001',
                                        "Sunny in Seattle this morning",sc)

    assert new_status_main is True
    assert exist_status_main is False

def test_update_status(empty_db):
    '''
    Updates the values of an existing status_id

    Requirements:
    - Returns False if there any errors.
    - Otherwise, it returns True.
    '''
    uc = main.init_user_collection(empty_db)
    sc = main.init_status_collection(empty_db)
    main.add_user('ckayx15','ckayx15@uw.edu','Kay','Xian',uc)
    main.add_status('ckayx15','ckay_00001','Test update status', sc)
    status_update_exist = main.update_status('ckay_00001','ckayx15',"OK update status",sc)
    status_non_exist_update = main.update_status('bbq_00001','ckayx15',"Fail update status",sc)

    assert status_update_exist is True
    assert status_non_exist_update is False


def test_delete_status(full_db):
    """
    Test delete_status calling from main.py
    """
    sc = main.init_status_collection(full_db)
    main.add_status('ckayx15', 'ckay_00002', "Test delete status", sc)
    del_exist_status = main.delete_status('ckay_00002', sc)
    del_non_exist_status = main.delete_status('bb1_00001',sc)

    assert del_exist_status is True
    assert  del_non_exist_status is False


def test_main_search_status(full_db):
    """
    Test search_status calling from main.py
    """
    sc = main.init_status_collection(full_db)
    search_exist_status = main.search_status('ckayx_00001', sc)
    search_non_exist_status = main.search_status('bbq_00001', sc)

    assert search_exist_status['_id'] == 'ckayx_00001'
    assert search_non_exist_status is None
