my $RESULT_HOME = $ENV{'RESULT_HOME'};
my $SCRIPT_HOME = "$ENV{'SPARK_HOME'}/scripts";

sub killTop {
    doOnAllNodes("killall top")
}

sub rmTopArtifacts {
    system("rm $RESULT_HOME/*.top");
}

sub killTopSocket {
    doOnAllNodes("pkill -f parse-top-output.py");
}

sub saveTopResult() {
    system("python3 $SCRIPT_HOME/figure1_experiment[io_wait_time]/parse-top-result.py -i $RESULT_HOME/*.top -o $RESULT_HOME/resultTop.csv");
}

1;
