my $RESULT_HOME = $ENV{'RESULT_HOME'};
    
sub setup {
    my ($tmpWorkdir) = @_;
    doOnAllNodes("mkdir -p /tmp/omranian");
    doOnAllNodes("mkdir -p $tmpWorkdir/spark-tmp");
    doOnAllNodes("mkdir -p /dev/shm/omranian");
    system("mkdir -p $RESULT_HOME/pdf")
}


1;
