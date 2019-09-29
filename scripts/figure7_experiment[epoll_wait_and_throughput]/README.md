# Experiment "Epoll Wait and I/O Throughput"

# Overview
The aim of this experiment is showing how the number of threads affect the proposed metrics: epoll wait time and I/O throughput and the combined metric in the Terasort application.

# Tools
The related data is written out by the spark executor itself when the dynamic solution is enabled. The related data for each executor can be found in: `$RESULT_HOME`/`appName`#`executorId`.dca which is then combined to `$RESULT_HOME/combined.dca` to have a single file with the data for all the executors and stages.

# Scripts

#### `../dca/combine.sh`
Combines the individual `.dca` files of each executor into a single file which holds the data for all the executors and stages. This script is called in `../common/spark/run_dynamic.pl` once an application is finished.

=================================================================

#### ` plot_epoll_throughput.py`
plots the final figure, based on the data file generated by  the `../dca/combine.sh` script.

#### Arguments
* --inputFile, -i: Absolute path of the file generated by the `../dca/combine.sh` script.
* --outputFile, -o: Absolute path of the directory where output files will be saved. The file name is formatted in the form of epoll_stage`stageNum`.pdf. For example, 'epoll_stage0' means the first stage of Terasort application.


# Requirements
This experiment does not have any special requirements. Make sure the general requirements are met.