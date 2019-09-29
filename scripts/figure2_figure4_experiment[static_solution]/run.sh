numberOfTimes=$1

# terasort
../common/spark/run_terasort.pl 100 $numberOfTimes 0 32 
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 16 
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 8
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 4
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 2 

# hibench
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 16 
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 8
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 4
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 2 

../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 16 
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 8
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 4
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 2 

../common/spark/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 1 32 16 
../common/spark/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 1 32 8
../common/spark/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 1 32 4
../common/spark/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 1 32 2