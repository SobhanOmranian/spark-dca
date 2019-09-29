sub setIoScheduler {
    my ($schedulerId) = @_;
    
    my $scheduler = "cfq";
    if ($schedulerId == 0){
        $scheduler = "cfq";
    }
    elsif ($schedulerId == 1) {
        $scheduler = "noop";
    }
    elsif ($schedulerId == 2) {
        $scheduler = "deadline";
    }

    doOnAllNodes("sudo /cm/shared/package/utils/bin/iosched $scheduler");
}

sub setIoSchedulerToDefault {
    setIoScheduler(0);
}

1;
