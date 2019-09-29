numberOfTimes=$1

# Terasort
# default
../common/spark/run_terasort.pl 100 $numberOfTimes 0 32 

# static
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 16 
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 8
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 4
../common/spark/run_terasort.pl 100 $numberOfTimes 1 32 2

# dynamic
../common/spark/run_terasort.pl 14 $numberOfTimes 0 32 0


# Aggregation
# default
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 0 32

# static
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 16 
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 8
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 4
../common/spark/run_hibench.pl sql aggregation gigantic run 100 $numberOfTimes 1 32 2  

# dynamic
../common/spark/run_hibench.pl sql aggregation gigantic run 14 $numberOfTimes 0 32 1

# Join
# default
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 0 32

# static
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 16 
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 8
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 4
../common/spark/run_hibench.pl sql join gigantic run 100 $numberOfTimes 1 32 2  

# dynamic
../common/spark/run_hibench.pl sql join gigantic run 14 $numberOfTimes 0 32 1


# PageRank
# default
../common/spark/run_hibench.pl websearch pagerank gigantic run 100 $numberOfTimes 0 32

# static
../common/spark/run_hibench.pl websearch pagerank gigantic run 100 $numberOfTimes 1 32 16 
../common/spark/run_hibench.pl websearch pagerank gigantic run 100 $numberOfTimes 1 32 8
../common/spark/run_hibench.pl websearch pagerank gigantic run 100 $numberOfTimes 1 32 4
../common/spark/run_hibench.pl websearch pagerank gigantic run 100 $numberOfTimes 1 32 2  

# dynamic
../common/spark/run_hibench.pl websearch pagerank gigantic run 14 $numberOfTimes 0 32 1