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
    aql = "UPSERT @user " \
          "INSERT @user " \
          "UPDATE {} IN users"

    bind = {
        "user": {"_key": user},
    }
    db.AQLQuery(aql, rawResults=False, bindVars=bind)

    # UPSERT Artists
    aql = "FOR doc IN @artists " \
          "UPSERT {_key: doc._key} " \
          "INSERT doc " \
          "UPDATE {} IN artists"
    bind = {
        "artists": artists,
    }
    db.AQLQuery(aql, rawResults=False, bindVars=bind)

    #  UPSERT Recordings
    aql = "FOR doc IN @recordings  " \
          "UPSERT {_key: doc._key} " \
          "INSERT doc " \
          "UPDATE {} IN recordings "
    bind = {
        "recordings": recordings
    }
    db.AQLQuery(aql, rawResults=False, bindVars=bind)


def add_edges(users_to_artists, users_to_recordings, artists_to_recordings):
    db = conn[DatabaseConstants.db_name]
    aql = "FOR doc IN @users_to_artists  " \
          "UPSERT {_key: doc._key} " \
          "INSERT doc " \
          "UPDATE {count: doc.count} IN users_to_artists "
    bind = {
        "users_to_artists": users_to_artists,
    }
    db.AQLQuery(aql, rawResults=False, bindVars=bind)

    aql = "FOR doc IN @users_to_recordings  " \
          "UPSERT {_key: doc._key} " \
          "INSERT doc " \
          "UPDATE {count: doc.count} IN users_to_recordings "
    bind = {
        "users_to_recordings": users_to_recordings,
    }
    db.AQLQuery(aql, rawResults=False, bindVars=bind)

    aql = "FOR doc IN @artists_to_recordings  " \
          "UPSERT {_key: doc._key} " \
          "INSERT doc " \
          "UPDATE {} IN artists_to_recordings "
    bind = {
        "artists_to_recordings": artists_to_recordings,
    }
    db.AQLQuery(aql, rawResults=False, bindVars=bind)


def setup():
    try:
        db = conn.createDatabase("MLHD_processing")
    except:
        db = conn["MLHD_processing"]
    db.createCollection(name=DatabaseConstants.users)
    db.createCollection(name=DatabaseConstants.artists)
    db.createCollection(name=DatabaseConstants.recordings)
    db.createCollection(name=DatabaseConstants.processed_chunks)

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
