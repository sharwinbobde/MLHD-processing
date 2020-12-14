# Music Listening Histories Dataset processing

This repository houses the code for doing the below tasks to process the Music Listening Histories dataset in a usable fashion.


## What is MLHD?
The Music Listening Histories Dataset (MLHD) is a large-scale collection of music listening events. It is sanitized and has MusicBrainx identifiers (MBID) for atists, releases and recordings for the listening events. 
At the moment of writing this description the dataset website is available [here](https://ddmal.music.mcgill.ca/research/The_Music_Listening_Histories_Dataset_(MLHD)/). The core listening files are available as a collection of 18 `.tar` files which when extracted give 576 `.tar` files in total (with `MLHD_386.tar` having no actual data)
The files are available on the [Globus System](https://app.globus.org/file-manager?origin_id=6e604070-3009-11eb-b16c-0ee0d5d9299f&origin_path=%2F), which is a platform for primarily sharing research data. 


## Getting Started
To setup the working environemnt please do the following:
* Install arangodb. Go to the [arangodb website](https://www.arangodb.com/) to download the *community edition*. The code in this repository has been tested with the distribution for Arch Linux (arangodb version `3.7.2`), using the arangodb starter.
* add the arangodb `bin` folder to the `PATH` variable for your system.
* Set values in config.py
* Set in which directory the arangodb database files will be stored by changing the path in `start_arangodb.sh`. Start arangodb by running the shell script through `./start_arangodb.sh`
* Create a python3 environment and select it. Learn how to do it [here](https://docs.python.org/3/library/venv.html). The code has been tested using python version `3.8.6`.
* Install the python libraries using the command `pip install -r requirements.txt`
* setup the database with the required collections by running the arangodb setup script `pyhton ./src/arangodb_functions.py`


## System Specifications
The code has been tested using the following system:*
* Alienware m15 R2
* Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
* MemTotal:       16149440 kB
* 2x primary NVMe SSD, 512 GB each. Here is the temporary extraction of the dataset files
* external HDD 4 TB. Here lie the datasets and database 
 

## Verifying Downloads
Verify whether the files are downloaded correctly by examining the SHA256 hashes for each of the `MLHD_###.tar` files.
Be sure to extract all 18 files in a single folder along with the `MLHD_sha256.txt` file.

Make sure your config file's `dataset_directory` value is properly set up.
Then run the script `./src/readMLHD.py` and select the option `Verify all files`.
You can also verify the hashes for files in a particular range by selecting the option `Verify particular range of files`.

## Read Files


## Add Listening Data to ArangoDB

## Add User Data to ArangoDB