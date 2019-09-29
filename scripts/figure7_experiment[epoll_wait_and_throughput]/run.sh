numberOfTimes=$1

# terasort (dynamic)
../common/spark/run_terasort.pl 14 $numberOfTimes 0 32 0

# hibench (dynamic)
../common/spark/run_hibench.pl sql aggregation gigantic run 14 $numberOfTimes 0 32 0

../common/spark/run_hibench.pl sql join gigantic run 14 $numberOfTimes 0 32 0

../common/spark/run_hibench.pl websearch pagerank large run 14 $numberOfTimes 0 32 0