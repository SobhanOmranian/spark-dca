# Self-Adaptive Spark (Spark-DCA)

This repository is used to store code, scripts and documentation for the Middleware'19 publication: "Self-adaptive Executors for Big Data Processing". The data files and the docs can be found in the following repository:
<https://doi.org/10.4121/uuid:38529ffe-00d0-42b0-9b3c-29d192262686>


# Abstract
The abstract can be found here.


# General Information
* Code: Most of the changes to the spark codebase are kept in the `core` module and package `org.apache.spark.dca` and its subpackages. Some additional changes had to be made to the Scheduler and Executor of Spark.

* Scripts: all the scripts used for gathering data and plotting are stored in the `scripts/` directory. For additional information on how to set up the experiments, refer to the readme file in that directory.

# Version information:
* Spark: 2.2
* Hadoop: 2.9.1
* Python: 3.7.1
* HiBench: 7.0


# How to build and run the experiments?
1. Clone the repository
2. Build Spark by executing `build.sh`.
3. Make sure the prerequisites in the readme file of the `scripts/` directory are met.
4. Navigate to the corresponding experiment in the `scripts/` directory and execute `./run.sh N` where `N` is the number of times the experiment should be repeated for each application. For instance `./run.sh 1` to only run each application once.
