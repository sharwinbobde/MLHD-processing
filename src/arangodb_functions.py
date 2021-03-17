#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

import inquirer
import requests
from config import DatabaseConstants
from pyArango.connection import *

import config

# Global db reference
conn = Connection(username=config.arangodb_user, password=config.arangodb_password)


class CursorProcessor:
    def __init__(self, aql, batchSize):
        self.db = conn[DatabaseConstants.db_name]
        self.aql = aql
        self.batchSize = batchSize
        self.cursor_id = None
        self.has_more = True

    def next_batch(self):
        if self.cursor_id is None:
            response = requests \
                .post("http://localhost:8529/_db/" + DatabaseConstants.db_name + "/_api/cursor",
                      json={
                          "query": self.aql,
                          'batchSize': self.batchSize
                      })
            if response.status_code != 201:
                print(response.json())
                raise Exception("Cursor AQL did not work")

            response = response.json()
            self.cursor_id = int(response["id"])
            self.has_more = response["hasMore"]
            return response["result"]

        elif self.has_more:
            url = "http://localhost:8529/_db/" + DatabaseConstants.db_name + "/_api/cursor/" + str(self.cursor_id)
            response = requests.put(url)
            if response.status_code != 200:
                print(response.json())
                raise Exception("Cursor AQL did not work")

            response = response.json()
            self.has_more = response["hasMore"]
            return response["result"]
        else:
            return None



def add_nodes(user, artists, recordings):
    db = conn[DatabaseConstants.db_name]
    # ===============================================
    # UPSERT Users
    transaction = '''
      function(params) {
          var db = require('@arangodb').db;
          db._query(`
            INSERT @user IN users OPTIONS {ignoreErrors: true}`,
            {user:params.user}
           )
    }
    '''
    bind = {
        "user": {"_key": user},
    }
    db.transaction({"write": ['users']}, transaction, params=bind)

    # ===============================================
    # UPSERT Artists
    transaction = '''
      function(params) {
          var db = require('@arangodb').db;
          db._query(`
          FOR doc IN @artists
           INSERT doc IN artists OPTIONS {ignoreErrors: true}`, 
            {artists:params.artists}
           )
    }
    '''
    bind = {
        "artists": artists,
    }
    db.transaction({"exclusive": ['artists']}, transaction, params=bind)

    # ===============================================
    #  UPSERT Recordings
    transaction = '''
      function(params) {
          var db = require('@arangodb').db;
          db._query(`
            FOR doc IN @recordings\n
            INSERT doc IN recordings OPTIONS {ignoreErrors: true}`,
            {recordings:params.recordings}
           )
    }
    '''
    bind = {
        "recordings": recordings
    }
    db.transaction({"exclusive": ['recordings']}, transaction, params=bind)


def add_edges(users_to_artists, users_to_recordings, artists_to_recordings):
    db = conn[DatabaseConstants.db_name]
    transaction = '''
      function(params) {
          var db = require('@arangodb').db;
          db._query(`
            FOR doc IN @users_to_artists  
            INSERT doc IN users_to_artists OPTIONS {ignoreErrors: true}`,
            {users_to_artists:params.users_to_artists}
           )
    }
    '''
    bind = {
        "users_to_artists": users_to_artists,
    }
    db.transaction({"exclusive": ['users_to_artists']}, transaction, params=bind)

    # ===============================================
    transaction = '''
      function(params) {
          var db = require('@arangodb').db;
          db._query(`
            FOR doc IN @users_to_recordings
            INSERT doc IN users_to_recordings OPTIONS {ignoreErrors: true}`,
            {users_to_recordings:params.users_to_recordings}
           )
    }
    '''
    bind = {
        "users_to_recordings": users_to_recordings,
    }
    db.transaction({"exclusive": ['users_to_recordings']}, transaction, params=bind)

    # ===============================================
    transaction = '''
      function(params) {
          var db = require('@arangodb').db;
          db._query(`
            FOR doc IN @artists_to_recordings
            INSERT doc IN artists_to_recordings OPTIONS {ignoreErrors: true}`,
            {artists_to_recordings:params.artists_to_recordings}
           )
    }
    '''
    bind = {
        "artists_to_recordings": artists_to_recordings,
    }
    db.transaction({"exclusive": ['artists_to_recordings']}, transaction, params=bind)


def get_count_recordings():
    aql = '''
        FOR doc IN recordings
        COLLECT WITH COUNT INTO length
        RETURN length
        '''
    return AQL(aql)
    # db = conn[DatabaseConstants.db_name]
    # result = db.AQLQuery(aql, rawResults=True)
    # return result.__next__()


def AQL(aql):
    db = conn[DatabaseConstants.db_name]
    result = db.AQLQuery(aql, rawResults=True)
    return result.__next__()


def setup():
    try:
        db = conn.createDatabase("MLHD_processing")
    except Exception:
        db = conn["MLHD_processing"]
    db.createCollection(name=DatabaseConstants.users)
    db.createCollection(name=DatabaseConstants.artists)
    db.createCollection(name=DatabaseConstants.recordings)
    db.createCollection(name=DatabaseConstants.ABz_low_level)

    db.createCollection("Edges", name=DatabaseConstants.users_to_artists)
    db.createCollection("Edges", name=DatabaseConstants.users_to_recordings)
    db.createCollection("Edges", name=DatabaseConstants.artists_to_recordings)


def clear_database():
    db = conn[DatabaseConstants.db_name]
    db.dropAllCollections()


def delete_edges_from_a_chunk(part_number: int):
    db = conn[DatabaseConstants.db_name]
    aql = '''
        FOR doc IN users_to_artists
            FILTER doc.part == ''' + str(part_number) + '''
            REMOVE doc IN users_to_artists OPTIONS { ignoreErrors: true }
        '''
    db.AQLQuery(aql)
    print("1/3 done")
    aql = '''
        FOR doc IN users_to_recordings
            FILTER doc.part == ''' + str(part_number) + '''
            REMOVE doc IN users_to_recordings OPTIONS { ignoreErrors: true }
        '''
    db.AQLQuery(aql)
    print("2/3 done")
    aql = '''
        FOR doc IN artists_to_recordings
            FILTER doc.part == ''' + str(part_number) + '''
            REMOVE doc IN artists_to_recordings OPTIONS { ignoreErrors: true }
        '''
    db.AQLQuery(aql)
    print("3/3 done")
    print("Job Completed\n")


if __name__ == '__main__':
    questions = [
        inquirer.List('choice',
                      message="What do you want to do? ",
                      choices=[".",
                               'Setup database',
                               "Remove Edges from a dataset chunk",
                               "Clear Database!!",
                               ],
                      carousel=True
                      ),
    ]
    answers = inquirer.prompt(questions)

    if answers["choice"] == "Setup database":
        print("Setting up the required collections in the database")
        setup()

    elif answers["choice"] == "Clear Database!!":
        answer = input("Please indicate approval enter: \"Yes\"\n")
        if answer.lower() == "yes":
            print("deleting all data")
            clear_database()
        else:
            print("Phew! everything is still there.")

    elif answers["choice"] == "Remove Edges from a dataset chunk":
        part = int(input("Please enter the part number (e.g. 26 for MLHD_026.tar): "))
        delete_edges_from_a_chunk(part)
