#
# Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands. Coded for Master's Thesis project.
#

# user: root
# pass: Happy2Help!
database_path="/run/media/sharwinbobde/SharwinThesis/agarodb_data"
#database_path="/home/sharwinbobde/tudelft-HPC-cluster-home/arangodb_data/"

arangodb --starter.mode single --starter.data-dir $database_path \
        --dbservers.rocksdb.write-buffer-size 100123400 \
        --dbservers.rocksdb.max-write-buffer-number 3 \
        --dbservers.rocksdb.total-write-buffer-size 1012340000 \
        --dbservers.rocksdb.dynamic-level-bytes false \



#        --dbservers.cache.size 20123400 \
#        --dbservers.rocksdb.block-cache-size 50000000 \
#        --dbservers.rocksdb.enforce-block-cache-size-limit true \