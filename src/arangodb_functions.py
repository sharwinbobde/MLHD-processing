#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

from pyArango.connection import *
import config
import inquirer

# Global db reference
conn = Connection(username=config.arangodb_user, password=config.arangodb_password)


# We declare this just for refactoring and avoiding incorrect literals.
class DatabaseConstants:
    db_name = "MLHD_processing"

    users = "users"
    artists = "artists"
    releases = "releases"
    recordings = "recordings"
    processed_chunks = "processed_chunks"

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
            UPSERT {_key: doc._key} INSERT doc UPDATE {count: doc.count} IN users_to_artists`,
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
            UPSERT {_key: doc._key} INSERT doc UPDATE {count: doc.count} IN users_to_recordings`,
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
            UPSERT {_key: doc._key} INSERT doc UPDATE {count: OLD.count + doc.count} IN artists_to_recordings`,
            {artists_to_recordings:params.artists_to_recordings}
           )
    }
    '''
    bind = {
        "artists_to_recordings": artists_to_recordings,
    }
    db.transaction({"exclusive": ['artists_to_recordings']}, transaction, params=bind)


def setup():
    try:
        db = conn.createDatabase("MLHD_processing")
    except:
        db = conn["MLHD_processing"]
    db.createCollection(name=DatabaseConstants.users)
    db.createCollection(name=DatabaseConstants.artists)
    db.createCollection(name=DatabaseConstants.recordings)

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
