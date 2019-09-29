numberOfTimes=$1

../common/spark/run_terasort.pl 100 $numberOfTimes 0 32
../common/spark/run_terasort.pl 100 $numberOfTimes 0 16 
../common/spark/run_terasort.pl 100 $numberOfTimes 0 8
../common/spark/run_terasort.pl 100 $numberOfTimes 0 4
../common/spark/run_terasort.pl 100 $numberOfTimes 0 2 

../common/terasort/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 0 32