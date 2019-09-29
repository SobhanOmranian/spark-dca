# Experiment "I/O Performance"

# Overview
The aim of this experiment is assessing how the number of threads affect the I/O throughput of Terasort in different stages of its execution on HDDs and SSDs.

# Tools
Same tools (iostat) have been used as the ones in figure 5 (see docs).

# Scripts

For gathering data, same scripts have been used as the ones in figure 5.

#### `plot_io_performance.py`
plots the final figure, based on the data file.

#### Arguments
* --inputFile, -i: Absolute path of the iostat generated file.
* --outputFile, -o: Absolute path of the directory where output files will be saved. The file name is formatted in the form of `appName`\_`stageNumber.pdf`. For example, 'terasort_s0' means the first stage of Terasort application.
* --diskType, -d: Type of the disk. Possible values are `hdd` and `ssd`.


# Requirements
* See the requirements of figure 5.
