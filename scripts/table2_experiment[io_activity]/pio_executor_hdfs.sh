#!/bin/bash
[ "$1" == "" ] && echo "Error: Missing PID" && exit 1
IO=/proc/$1/io          # io data of PID

HDFS_PROCESS_ID=`jps | grep 'DataNode' | awk '{print $1}'`
[[ $HDFS_PROCESS_ID = ""  ]] && echo "Error: DataNode Process Not Found!" && exit 1
IO_HDFS=/proc/$HDFS_PROCESS_ID/io          # io data of HDFS


[ ! -e "$IO_HDFS" ] && echo "Error: PID of HDFS does not exist" && exit 2

[ ! -e "$IO" ] && echo "Error: PID does not exist" && exit 2

OUTPUT_FILE_NAME=$2
APP_NAME=$3
EXECUTOR_ID=$4
I=1                     # interval in seconds
SECONDS=0               # reset timer
TOTAL_READ_EXECUTOR=0
TOTAL_WRITE_EXECUTOR=0
TOTAL_READ_HDFS=0
TOTAL_WRITE_HDFS=0
RESULT_PATH=$RESULT_HOME
HOST_NAME=`hostname`

exec 3<> "$RESULT_PATH/${OUTPUT_FILE_NAME}.pio"
echo "appName,nodeName,executorId,executorTotalReadKb,executorTotalWriteKb,hdfsTotalReadKb,hdfsTotalWriteKb" >&3

show_final_result () {
        #echo "App [$APP_NAME] Node [$HOST_NAME] EXECUTOR => FINAL READ = $TOTAL_READ_EXECUTOR and FINAL WRITE = $TOTAL_WRITE_EXECUTOR" >&3
        #echo "App [$APP_NAME] Node [$HOST_NAME] HDFS => FINAL READ = $TOTAL_READ_HDFS and FINAL WRITE = $TOTAL_WRITE_HDFS" >&3
        echo "$APP_NAME,$HOST_NAME,$EXECUTOR_ID,$TOTAL_READ_EXECUTOR,$TOTAL_WRITE_EXECUTOR,$TOTAL_READ_HDFS,$TOTAL_WRITE_HDFS" >&3
}

echo "Watching command $(cat /proc/$1/comm) with PID $1"

IFS=" " read rchar wchar syscr syscw rbytes wbytes cwbytes < <(cut -d " " -f2 $IO | tr "\n" " ")
IFS=" " read rchar_hdfs wchar_hdfs syscr_hdfs syscw_hdfs rbytes_hdfs wbytes_hdfs cwbytes_hdfs < <(cut -d " " -f2 $IO_HDFS | tr "\n" " ")

while [ -e $IO ]; do
    IFS=" " read rchart wchart syscrt syscwt rbytest wbytest cwbytest < <(cut -d " " -f2 $IO | tr "\n" " ")
    IFS_2=" " read rchart_hdfs wchart_hdfs syscrt_hdfs syscwt_hdfs rbytest_hdfs wbytest_hdfs cwbytest_hdfs < <(cut -d " " -f2 $IO_HDFS | tr "\n" " ")

    S=$SECONDS
    [ $S -eq 0 ] && continue

cat << EOF
rchar:                 $((($rchart-$rchar)/1024/1024/$S)) MByte/s
wchar:                 $((($wchart-$wchar)/1024/1024/$S)) MByte/s
syscr:                 $((($syscrt-$syscr)/1024/1024/$S)) MByte/s
syscw:                 $((($syscwt-$syscw)/1024/1024/$S)) MByte/s
read_bytes:            $((($rbytest-$rbytes)/1024/1024/$S)) MByte/s
write_bytes:           $((($wbytest-$wbytest)/1024/1024/$S)) MByte/s
cancelled_write_bytes: $((($cwbytest-$cwbytes)/1024/1024/$S)) MByte/s
batch_read_bytes:      $(($rbytest / 1024))
batch_write_bytes:     $(($wbytest /1024))
EOF
    echo

cat << EOF
rchar_hdfs:                 $((($rchart_hdfs-$rchar_hdfs)/1024/1024/$S)) MByte/s
wchar_hdfs:                 $((($wchart_hdfs-$wchar_hdfs)/1024/1024/$S)) MByte/s
syscr_hdfs:                 $((($syscrt_hdfs-$syscr_hdfs)/1024/1024/$S)) MByte/s
syscw_hdfs:                 $((($syscwt_hdfs-$syscw_hdfs)/1024/1024/$S)) MByte/s
read_bytes_hdfs:            $((($rbytest_hdfs-$rbytes_hdfs)/1024/1024/$S)) MByte/s
write_bytes_hdfs:           $((($wbytest_hdfs-$wbytest_hdfs)/1024/1024/$S)) MByte/s
cancelled_write_bytes_hdfs: $((($cwbytest_hdfs-$cwbytes_hdfs)/1024/1024/$S)) MByte/s
batch_read_bytes_hdfs:      $(($rbytest_hdfs / 1024))
batch_write_bytes_hdfs:     $(($wbytest_hdfs /1024))
EOF
    echo

    TOTAL_READ_EXECUTOR=$(($rbytest/1024))
    TOTAL_WRITE_EXECUTOR=$(($wbytest/1024))
    TOTAL_READ_HDFS=$((($rbytest_hdfs-$rbytes_hdfs)/1024))
    TOTAL_WRITE_HDFS=$((($wbytest_hdfs-$wbytes_hdfs)/1024))
    #echo "App [$APP_NAME] Node [$HOST_NAME] EXECUTOR => READ = $TOTAL_READ_EXECUTOR and WRITE = $TOTAL_WRITE_EXECUTOR" >&3
    #echo "App [$APP_NAME] Node [$HOST_NAME] HDFS => READ = $TOTAL_READ_HDFS and WRITE = $TOTAL_WRITE_HDFS" >&3
    sleep $I
done
show_final_result
3>&-
