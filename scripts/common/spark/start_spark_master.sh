#!/bin/bash
$SPARK_HOME/sbin/start-master.sh --webui-port 8082
$SPARK_HOME/sbin/start-history-server.sh
