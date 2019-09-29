numberOfTimes=$1

# terasort (default)
../common/spark/run_terasort.pl 100 $numberOfTimes 0 32

# hibench (default)
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 0 32
