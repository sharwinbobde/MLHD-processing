#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

# parameters to tune considering memory + cpu constraints
n_cpu = 12
# memory_avail_GB = 4.5

# dataset_directory = "/run/media/sharwinbobde/SharwinThesis/MLHD/"
dataset_directory = "/run/media/sharwinbobde/SharwinBackup/MLHD/"
temp_extraction_destination = "/home/sharwinbobde/temp/MLHD_temp"
# separate partition on the SSD

arangodb_user = "root"
arangodb_password = "Happy2Help!"

# user should've heard/encountered this song more than these many times to be counted in the graph database.
# Keep in mind the dataset is already sanitized and all events have been heard > 30s
listen_lower_threshold = 25

processes_per_chunk = n_cpu


# =====================================================================
# Do NOT change these

invalid_field_presence_codes = \
    [
        0,  # nothing present
        2,  # only release present
    ],

AcousticBrainz_max_MBIDs_per_request = 25
AcousticBrainz_rate_limit_calls = 10
AcousticBrainz_rate_limit_period = 10  # 10 seconds
AcousticBrainz_rate_limit_exponential_backoff_max_attempts = 10
