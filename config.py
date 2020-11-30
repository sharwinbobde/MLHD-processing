#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

# parameters to tune considering memory + cpu constraints
n_cpu = 12
# memory_avail_GB = 4.5

dataset_directory = "/run/media/sharwinbobde/SharwinThesis/MLHD_test/"
temp_extraction_destination = "/run/media/sharwinbobde/3ab7f23d-ffe7-4af6-a60f-7eb1e64c383c/home/testpartition/MLHD_temp"
# separate partition on the SSD

arangodb_user = "root"
arangodb_password = "Happy2Help!"

# user should've heard/encountered this song more than these many times to be counted in the graph database.
# Keep in mind the dataset is already sanitized and all events have been heard > 30s
listen_lower_threshold = 25

# =====================================================================
# Do NOT change these
threads_per_chunk = 1

invalid_field_presence_codes = \
    [
        0,  # nothing present
        2,  # only release present
    ],