numberOfTimes=$1

../common/spark/run_terasort.pl 100 $numberOfTimes 32
../common/spark/run_terasort.pl 100 $numberOfTimes 16 
../common/spark/run_terasort.pl 100 $numberOfTimes 8
../common/spark/run_terasort.pl 100 $numberOfTimes 4
../common/spark/run_terasort.pl 100 $numberOfTimes 2 