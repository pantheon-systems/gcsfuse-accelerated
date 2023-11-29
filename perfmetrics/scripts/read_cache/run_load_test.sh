#!/bin/bash
# Copyright 2023 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# echo on
set -x

set -e

if [[ -z "${WORKING_DIR}" ]]; then
  echo "Please set the working directory..."
  exit 1
fi

num_of_threads=${1:-40}
ONE_MILLION=1000000
FIFTY_K=50000

cd $WORKING_DIR/gcsfuse/perfmetrics/scripts/read_cache/

# For file-size = 1K and block-size = 1K
./mount_gcsfuse.sh -s 950 -c /mnt/disks/local_ssd
workload_dir=$WORKING_DIR/gcs/1K
mkdir -p $workload_dir
number_of_files_per_thread=$((ONE_MILLION / num_of_threads))
./run_read_cache_fio_workload.sh -e 5 -n $number_of_files_per_thread -t $num_of_threads -b 1K -s 1K -d $workload_dir

# For file-size = 128K and block-size = 32K
./mount_gcsfuse.sh -s 12000 -c /mnt/disks/local_ssd
workload_dir=$WORKING_DIR/gcs/128K
mkdir -p $workload_dir
number_of_files_per_thread=$((ONE_MILLION / num_of_threads))
./run_read_cache_fio_workload.sh -e 5 -n $number_of_files_per_thread -t $num_of_threads -b 32K -s 128K -d $workload_dir

# For file-size = 1M and block-size = 256K
./mount_gcsfuse.sh -s 950000 -c /mnt/disks/local_ssd
workload_dir=$WORKING_DIR/gcs/1M
mkdir -p $workload_dir
number_of_files_per_thread=$((ONE_MILLION / num_of_threads))
./run_read_cache_fio_workload.sh -e 5 -n $number_of_files_per_thread -t $num_of_threads -b 256K -s 1M -d $workload_dir

# For file-size = 100M and block-size = 1M
./mount_gcsfuse.sh -s 4700000 -c /mnt/disks/local_ssd
workload_dir=$WORKING_DIR/gcs/100M
mkdir -p $workload_dir
number_of_files_per_thread=$((FIFTY_K / num_of_threads))
./run_read_cache_fio_workload.sh -e 5 -n $number_of_files_per_thread -t $num_of_threads -b 1M -s 100M -d $workload_dir

# Random read for file-size = 1M and block-size = 256K
./mount_gcsfuse.sh -s 950000 -c mnt/disks/local_ssd
workload_dir=$WORKING_DIR/gcs/1M
mkdir -p $workload_dir
number_of_files_per_thread=$((ONE_MILLION / num_of_threads))
./run_read_cache_fio_workload.sh -e 5 -n $number_of_files_per_thread -t $num_of_threads -b 256K -s 1M -r randread -d $workload_dir

# Random read for file-size = 100M and block-size = 1M
./mount_gcsfuse.sh -s 4700000 -c /mnt/disks/local_ssd
workload_dir=$WORKING_DIR/gcs/100M
mkdir -p $workload_dir
number_of_files_per_thread=$((FIFTY_K / num_of_threads))
./run_read_cache_fio_workload.sh -e 5 -n $number_of_files_per_thread -t $num_of_threads -b 1M -s 100M -r randread -d $workload_dir

# Go back to the old working directory.
cd -