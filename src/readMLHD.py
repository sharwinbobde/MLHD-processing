#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

import os
from os import listdir
from os.path import isfile, join
import tarfile
import gzip
import shutil
import threading
import config
import re
from tqdm import tqdm
from src.Scrobble import ScrobbleProcessor
import inquirer
import pandas as pd
import hashlib
import numpy as np
import multiprocessing

np.printoptions()


def threaded_scrobble_processing(loc):
    part = int(re.search(r"(?<=MLHD_)[\d]*(?=\/)", loc).group(0))
    inner_file = open(loc, 'rb')
    scrobble_tsv_filename = loc + ".tsv"
    # unzip scrobble file
    with gzip.open(inner_file, mode='rb') as f_in:
        with open(scrobble_tsv_filename, mode="wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Process files
    processor = ScrobbleProcessor(scrobble_tsv_filename, part)
    processor.start()
    return


def process_tar(filename: str, temp_dest: str):
    head, tail = os.path.split(filename)
    temp_dest += '/' + re.search(r"(?!\/)[^\/\.]*(?=\.)", tail).group(0)
    f = open(filename, 'rb')

    # extract large file
    tar = tarfile.open(fileobj=f, mode='r:')  # Unpack tar
    tar.extractall(temp_dest)
    scrobble_tars = []
    for tarinfo in tar:
        scrobble_tars.append(tarinfo.name)

    pool_args = []
    for scrobble_tar in scrobble_tars:
        loc = temp_dest + '/' + scrobble_tar
        pool_args.append(loc)

    print("Running Processes")
    pool = multiprocessing.Pool(config.processes_per_chunk)
    for _ in tqdm(pool.imap_unordered(threaded_scrobble_processing, pool_args), total=len(pool_args)):
        pass

    pool.close()
    pool.join()

    tar.close()
    # Cleanup
    shutil.rmtree(temp_dest, ignore_errors=True)


def compare_sha256_digest(file_path, known_digest):
    # Source: https://stackoverflow.com/a/55542529
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)
    if h.hexdigest() == known_digest:
        print(file_path + " \t [Passed] sha256 test")
    else:
        print(file_path + " \t [FAILED] sha256 test; Download Again!!")


def verify_dataset_integrity(consider_missing_files=False):
    sha256_df = pd.read_csv(config.dataset_directory + "MLHD_sha256.txt", header=None, sep='\t')
    threads = []
    for index, (file_sha256, name) in sha256_df.iterrows():
        filename = config.dataset_directory + name
        if os.path.isfile(filename):
            t = threading.Thread(target=compare_sha256_digest, args=(filename, file_sha256))
            t.start()
            threads.append(t)
            if len(threads) > config.n_cpu and len(threads) % config.n_cpu == 0:
                for t in threads[-config.n_cpu: -config.n_cpu + int(config.n_cpu / 2)]:
                    t.join()
        else:
            if consider_missing_files:
                print(name + "does not exist in the dataset directory")
    for t in threads:
        t.join()
    print("Finished")


def verify_dataset_integrity_within_range(start_, end_, consider_missing_files=False):
    sha256_df = pd.read_csv(config.dataset_directory + "MLHD_sha256.txt", header=None, sep='\t')
    threads = []
    for index, (file_sha256, name) in sha256_df.iterrows():
        if index < start_ or index > end_:
            continue

        filename = config.dataset_directory + name
        if os.path.isfile(filename):
            t = threading.Thread(target=compare_sha256_digest, args=(filename, file_sha256))
            t.start()
            threads.append(t)
            if len(threads) > config.n_cpu and len(threads) % config.n_cpu == 0:
                for t in threads[-config.n_cpu: -config.n_cpu + int(config.n_cpu / 2)]:
                    t.join()
        else:
            if consider_missing_files:
                print(name + "does not exist in the dataset directory")
    for t in threads:
        t.join()
    print("Finished")


if __name__ == '__main__':
    questions = [
        inquirer.List('choice',
                      message="What do you want to do?",
                      choices=[
                          ".",
                          'Verify all files',
                          'Verify particular range of files',
                          'Transfer MLHD to Graph database',
                          'Transfer [range of] MLHD files to Graph database'
                      ],
                      carousel=True
                      ),
    ]
    answers = inquirer.prompt(questions)

    if answers["choice"] == "Verify all files":
        print("Verifying files...\t This may take some time")
        verify_dataset_integrity(consider_missing_files=True)

    elif answers["choice"] == 'Verify particular range of files':
        start = int(input("Input start index:"))
        end = int(input("Input end index:"))
        print("Verifying files...\t This may take some time")
        verify_dataset_integrity_within_range(start, end, consider_missing_files=True)

    elif answers["choice"] == "Transfer MLHD to Graph database":
        dataset_path = config.dataset_directory
        temp_extract_dest = config.temp_extraction_destination
        files = [f for f in listdir(dataset_path) if isfile(join(dataset_path, f))]

        batch = 0
        for file in files:
            if file == 'MLHD_sha256.txt':
                continue

            thread = threading.Thread(target=process_tar, args=(dataset_path + file, temp_extract_dest))
            print("Starting for " + file)
            thread.start()
            # Wait for this chunk to be over
            thread.join()
        print("All tasks have Finished")

    elif answers["choice"] == "Transfer [range of] MLHD files to Graph database":
        dataset_path = config.dataset_directory
        temp_extract_dest = config.temp_extraction_destination
        files = [f for f in listdir(dataset_path) if isfile(join(dataset_path, f))]

        start = int(input("Input start index:"))
        end = int(input("Input end index (included):"))

        for num in range(start, end + 1):
            file = 'MLHD_' + str(num).zfill(3) + '.tar'

            thread = threading.Thread(target=process_tar, args=(dataset_path + file, temp_extract_dest))
            print("Starting for " + file)
            thread.start()
            # Wait for this chunk to be over
            thread.join()
        print("All tasks have Finished")
