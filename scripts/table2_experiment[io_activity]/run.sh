numberOfTimes=$1

# terasort (default)
../common/spark/run_terasort.pl 100 $numberOfTimes 0 32

# hibench (default)
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl websearch pagerank large run 100 $numberOfTimes 0 32

../common/spark/run_hibench.pl ml bayes large run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl ml lda large run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl graph nweight large run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl sql scan large run 100 $numberOfTimes 0 32
../common/spark/run_hibench.pl ml svm large run 100 $numberOfTimes 0 32
