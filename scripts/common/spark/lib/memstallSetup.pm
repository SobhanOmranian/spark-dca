sub killMemstall {
    doOnAllNodes("killall perf")
}

sub rmMemstallArtifacts {
    system("rm /home/omranian/results/*.memstall");
}

sub killMemstallSocket {
    doOnAllNodes("pkill -f parse-memstall-output.py");
}

sub saveMemstallResult() {
    system("cat ~/results/*.memstall | awk '{if (NR>1 && \$0 ~/nodeId/) { next } else print}' > ~/results/combined.memstall");
    system("python3 /home/omranian/scripts/memstall/parse-memstall-result.py -f /home/omranian/results/combined.memstall");
}

1;
