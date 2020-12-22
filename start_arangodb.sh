#
# Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands. Coded for Master's Thesis project.
#

# user: root
# pass: Happy2Help!
database_path="/run/media/sharwinbobde/SharwinThesis/agarodb_data"
#database_path="/home/sharwinbobde/tudelft-HPC-cluster-home/arangodb_data/"

arangodb --starter.mode single --starter.data-dir $database_path \
        --dbservers.rocksdb.write-buffer-size 25000000 \
        --dbservers.rocksdb.max-write-buffer-number 2 \
        --dbservers.rocksdb.total-write-buffer-size 50000000 \
        --dbservers.rocksdb.dynamic-level-bytes false \
        --dbservers.cache.size 50123400 \
        --dbservers.rocksdb.block-cache-size 4000000 \
        --dbservers.rocksdb.enforce-block-cache-size-limit true

#        &

#        --dbservers.rocksdb.num-threads-priority-high 12 \
#        --dbservers.rocksdb.num-threads-priority-low 12 \