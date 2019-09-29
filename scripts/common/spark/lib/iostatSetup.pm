my $RESULT_HOME = $ENV{'RESULT_HOME'};
my $SCRIPT_HOME = "$ENV{'SPARK_HOME'}/scripts";

sub killIostat {
    doOnAllNodes("killall iostat")
}


sub rmIostatArtifacts {
    system("rm $RESULT_HOME/*.iostat");
}

sub killIostatSocket {
    doOnAllNodes("pkill -f parse-iostat-output.py");
}

sub saveIostatResult {
    my ($appName) = @_;
    system("cat $RESULT_HOME/*.iostat | awk '{if (NR>1 && \$0 ~/nodeId/) { next } else print}' >> $RESULT_HOME/iostat.combined");
    system("sed -i'' -e '2,\${ /nodeId/d}' $RESULT_HOME/iostat.combined");
    system("python3 $SCRIPT_HOME/iostat/parse-iostat-result.py -i $RESULT_HOME/iostat.combined -o $RESULT_HOME/resultIostat.csv  -a $appName");
}

1;
