numberOfTimes=$1

# default
../common/spark/run_terasort.pl 100 $numberOfTimes 0 32 

# static
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 16 
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 8
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 4
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 2

# dynamic
../common/spark/run_terasort.pl 14 $numberOfTimes 0 32 0