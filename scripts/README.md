# Prerequisites

1. Set the following environment variables:

  * `SPARK_HOME`: the root directory of the Spark instance.
  * `HADOOP_HOME`: The root directory of the Hadoop instance.
  * `RESULT_HOME`: The root directory which will contain all the data files and plots.
  * `SPARK_WORK_DIR` : The root directory which Spark uses to store its temporary files.
  * `LOG_HOME`: The root directory which contains the log files of some of the scripts on each node.


2. Rewrite the script which clears the buffer cache:
Since most of the experiments deal with I/O, one needs to make sure the OS buffer cache is cleared after each run, so that the I/O request actually reach the disk. The script responsible for this is in `common/terasort/clear_buffer_cache.sh`. Since this operation requires sudo access, the user must replace this line with their own script which flushes the buffer cache.
On Linux, this is typically done by running the following commands:
`/bin/sync` and
`/bin/echo 3 > /proc/sys/vm/drop_caches`

3. Setup slaves in `common/slaves`:
    List the nodes (one per line) on which Spark executors will be launched.
