numberOfTimes=$1
INFILE="/tmp/omranian/largefile"
BLOCK_SIZE=134000000
COUNT=229

THREAD_NUM=1
IFS=$'\n' read -d '' -r -a nodes < slaves

exec 3<> $RESULT_HOME/resultIoVariability.csv

echo "nodeNum,blockSize,blockCount,threadNum,writeTime,readTime">&3

for i in {1..$numberOfTimes}
do
    for node in ${nodes[@]}
    do
#         if [[ $i -eq 302 ]]; then
#             continue
#         fi
#         if [[ $i -eq 301 ]]; then
#             continue
#         fi
#         if [[ $i -eq 311 ]]; then
#             continue
#         fi
        NODE_NUM=$node
        echo $NODE_NUM
        ssh $NODE_NUM 'sudo /cm/shared/package/utils/bin/drop_caches'

        SECONDS=0

        # Generate file
        ssh $NODE_NUM "dd oflag=dsync  if=/dev/zero of=$INFILE bs=$BLOCK_SIZE count=$COUNT"

        WRITE_TIME=$SECONDS

        ssh $NODE_NUM 'sudo /cm/shared/package/utils/bin/drop_caches'

        #BLOCK_NUM=
        INPUT_SIZE=$( ssh $NODE_NUM "stat --format %s $INFILE"  )
        BLOCK_NUM=$(( $INPUT_SIZE / $BLOCK_SIZE ))
        RECORDS_READ_NUM=$(( $INPUT_SIZE / $THREAD_NUM  ))
        echo "Spawning $THREAD_NUM threads each reading $RECORDS_READ_NUM records."

        SECONDS=0
        for i in `seq 1 $THREAD_NUM`;
        do
            if (( $i == 1 )); then
                echo "1"
                ssh $NODE_NUM "dd if=$INFILE  iflag=skip_bytes,count_bytes bs=$BLOCK_SIZE  count=$RECORDS_READ_NUM of=/dev/null"
            else
                OFFSET=$(( $RECORDS_READ_NUM * ($i-1) ))
                echo "Thread [$i] => Skipping to record: $OFFSET"
                ssh $NODE_NUM "dd if=$INFILE iflag=skip_bytes,count_bytes  bs=$BLOCK_SIZE skip=$OFFSET count=$RECORDS_READ_NUM of=/dev/null &"
            fi
        done
        wait

        echo "BLOCK_SIZE:$BLOCK_SIZE, BLOCK_COUNT:$COUNT, THREAD_NUM:$THREAD_NUM, WRITE_TIME=$WRITE_TIME, READ_TIME:$SECONDS"
        echo "$NODE_NUM,$BLOCK_SIZE,$COUNT,$THREAD_NUM,$WRITE_TIME,$SECONDS" >&3

    done
done
3>&-
