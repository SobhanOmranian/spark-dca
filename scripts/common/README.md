# Information

1. Terasort data: For generating the terasort data, one should run the `terasort/generate_input` script. The final data will be put on the running instance of Hadoop. (Set by `HADOOP_HOME`). There are two main variables to set in this script:
  * `records`: how many records will be in the file. For example 300000000 means 30 GB.
  * `workdir`: the directory which holds the temporary files before they are put in HDFS.
