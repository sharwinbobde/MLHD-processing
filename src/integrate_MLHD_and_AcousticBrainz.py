#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.
import inquirer
import src.arangodb_functions as af
import config
from tqdm import tqdm
from src.AcousticBrainzAPI import *


def forEach_batch(func):
    db = af.conn[af.DatabaseConstants.db_name]
    aql = "FOR doc in recordings RETURN doc._key"
    total_recordings = af.get_count_recordings()
    cp = af.CursorProcessor(aql, config.AcousticBrainz_max_MBIDs_per_request)
    print([total_recordings])
    pbar = tqdm(total=total_recordings)
    while True:
        batch = cp.next_batch()
        if batch is None:
            break
        pbar.update(len(batch))
        func(batch)
    pbar.close()


def add_ABz_low_level_to_arangodb(batch):
    result = get_low_level_features(batch)
    for i in batch:
        try:
            res = result[i]
            print(str(i) + " has records")

        except KeyError:
            # No AcousticBrainz records
            # print(str(i)+" doesnt have records!!")
            print(".", end="")


if __name__ == '__main__':

    questions = [
        inquirer.List('choice',
                      message="What do you want to do for the recordings in MLHD graph present on arangodb?",
                      choices=[".", 'Get AcousticBrainz low-level features'],
                      carousel=True),
    ]
    answers = inquirer.prompt(questions)

    if answers["choice"] == "Get AcousticBrainz low-level features":
        print("This may take some time...\t grab a coffee while you are at it.")
        forEach_batch(add_ABz_low_level_to_arangodb)
