#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

from pyArango.connection import *
from pyArango.database import *
from pyArango.query import *
import config
import inquirer
import requests

# Global db reference
conn = Connection(username=config.arangodb_user, password=config.arangodb_password)


class CursorProcessor:
    def __init__(self, aql, batchSize):
        self.db = conn[DatabaseConstants.db_name]
        self.aql = aql
        self.batchSize = batchSize
        self.cursor_id = None
        self.has_more = False

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


# We declare this just for refactoring and avoiding incorrect literals.
class DatabaseConstants:
    db_name = "MLHD_processing"

    users = "users"
    artists = "artists"
    releases = "releases"
    recordings = "recordings"
    ABz_low_level = "AcousticBrainz_LowLevel"

    users_to_artists = "users_to_artists"
    users_to_recordings = "users_to_recordings"
    artists_to_recordings = "artists_to_recordings"


def add_nodes(user, artists, recordings):
    db = conn[DatabaseConstants.db_name]
    # ===============================================
    # UPSERT Users
    transaction = '''
      function(params) {
          var db = require('@arangodb').db;
          db._query(`
            UPSERT @user INSERT @user UPDATE {} IN users`,
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
           UPSERT {_key: doc._key} INSERT doc UPDATE {} IN artists`, 
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
            UPSERT {_key: doc._key} INSERT doc UPDATE {} IN recordings`,
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
            UPSERT {_key: doc._key} INSERT doc UPDATE {years: doc.years} IN users_to_artists`,
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
            UPSERT {_key: doc._key} INSERT doc UPDATE {years: doc.years} IN users_to_recordings`,
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
            UPSERT {_key: doc._key} INSERT doc UPDATE {} IN artists_to_recordings`,
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


if __name__ == '__main__':
    questions = [
        inquirer.List('choice',
                      message="What do you want to do?",
                      choices=[".", 'Setup database', "Clear Database!!"],
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
