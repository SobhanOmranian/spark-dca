my $RESULT_HOME = $ENV{'RESULT_HOME'};
my $SCRIPT_HOME = "$ENV{'SPARK_HOME'}/scripts";

sub killMpstat {
    doOnAllNodes("killall mpstat")
}

#sub killMpstatParser() {
    #doOnAllNodes("pkill -SIGINT mpstat");
#}

sub rmMpstatArtifacts {
    system("rm $RESULT_HOME/*.mpstat");
}

sub killMpstatSocket {
    doOnAllNodes("pkill -f parse-mpstat-output.py");
}

sub saveMpstatResult() {
    system("python3 $SCRIPT_HOME/figure1_experiment[io_wait_time]/parse-mpstat-result.py -i $RESULT_HOME/*.mpstat -o $RESULT_HOME/resultMpstat.csv");
}

1;
