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

np.printoptions()


def threaded_scrobble_processing(filename):
    # print(filename)
    processor = ScrobbleProcessor(filename)
    processor.start()


def read_tar(filename: str, temp_dest: str):
    head, tail = os.path.split(filename)
    temp_dest += '/' + re.search(r"(?!\/)[^\/\.]*(?=\.)", tail).group(0)
    f = open(filename, 'rb')

    # extract large file
    tar = tarfile.open(fileobj=f, mode='r:')  # Unpack tar
    tar.extractall(temp_dest)
    threads = []
    count = 0
    scrobble_tars = []
    for tarinfo in tar:
        scrobble_tars.append(tarinfo.name)
    for scrobble_tar in tqdm(scrobble_tars, ncols=70, smoothing=0.2, disable=False):
        loc = temp_dest + '/' + scrobble_tar
        inner_file = open(loc, 'rb')
        scrobble_tsv_filename = loc + ".tsv"

        # unzip scrobble file
        with gzip.open(inner_file, mode='rb') as f_in:
            with open(scrobble_tsv_filename, mode="wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # process file parallelly
        t = threading.Thread(target=threaded_scrobble_processing, args=(scrobble_tsv_filename,))
        count += 1
        t.start()
        threads.append(t)

        # no point processing hundreds of DataFrames in parallel
        if count > config.threads_per_chunk and count % config.threads_per_chunk == 0:
            for t in threads[-config.threads_per_chunk: -config.threads_per_chunk + int(config.threads_per_chunk / 2)]:
                t.join()

    # wait for all threads to end. Wait for all because the upper join() logic can be edited.
    for t in threads:
        t.join()
    tar.close()
    # Cleanup
    # shutil.rmtree(temp_dest, ignore_errors=True)


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


def verify_dataset_integrity_within_range(start, end, consider_missing_files=False):
    sha256_df = pd.read_csv(config.dataset_directory + "MLHD_sha256.txt", header=None, sep='\t')
    threads = []
    for index, (file_sha256, name) in sha256_df.iterrows():
        if index < start or index > end:
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
                          'Transfer MLHD to Graph database'
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

            thread = threading.Thread(target=read_tar, args=(dataset_path + file, temp_extract_dest))
            print("Starting for " + file)
            thread.start()
            # Wait for this chunk to be over
            thread.join()
        print("All tasks have Finished")
