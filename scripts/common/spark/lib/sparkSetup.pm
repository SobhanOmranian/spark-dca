use strict;
use warnings;
use FindBin;

my $master_url = "spark://fs3.cm.cluster:7077";
my $base_spark_dir = "$ENV{'SPARK_HOME'}";
my $RESULT_HOME = $ENV{'RESULT_HOME'};

my @all_nodes = qw();
my @executor_nodes = qw();
readSlaves();

sub readSlaves {
    my $slave_path = "$FindBin::RealBin/..";
    chdir($slave_path);
#     print("slave_path: $slave_path\n");
    open my $in, "<:encoding(utf8)", "slaves" or die "$slave_path: $!";
    while (my $line = <$in>) {
        chomp $line;
        $line = clean($line);
        push(@all_nodes, $line);
    }
    close $in;
    @executor_nodes = @all_nodes;
}

sub clean {

    my $text = shift;

    $text =~ s/\n//g;
    $text =~ s/\r//g;

    return $text;
}

sub startIoStat {
    my $app_name = $_[0];
    my $executor_node = "";
      foreach (@executor_nodes){
          $executor_node = $_;
          system("ssh omranian\@${executor_node} '/var/scratch/omranian/git/iostat-csv/iostat-csv.sh | tee /home/omranian/logs/executor/iostat-${executor_node}-${app_name}.csv > /dev/null &'");
    }
}


sub stopIoStat() {
   doOnAllNodes("killall iostat");
}

sub getNumberOfNodes {
    return scalar @executor_nodes;
}

sub getAppIdFromFileName() {
    my $file_name = `ls $RESULT_HOME/*.pio | tail -n1 | tr -d '\n' | tr -d "'"`;
    #$file_name = `echo \${$file_name//'}`;

    #my $app_id = `FILE_NAME=$file_name; echo \${FILE_NAME%.pio}`;
    my $app_id = `FILE_NAME=$file_name; echo \${FILE_NAME%.pio} | tr -d '\n'`;
    #print($app_id);
    $app_id = `APP_ID=$app_id; echo \${APP_ID##*app}`;
    #print($app_id_2);
    $app_id = `echo "app$app_id" | tr -d '\n'`;
    return $app_id;
}
sub getFullAppNameFromFileName {
    my $file_name = `ls $RESULT_HOME/*.top | tail -n1 | tr -d '\n' | tr -d "'"`;
    #$file_name = `echo \${$file_name//'}`;
    #my $app_id = `FILE_NAME=$file_name; echo \${FILE_NAME%.pio}`;
    my $app_id = `FILE_NAME=$file_name; basename \${FILE_NAME%.top} |  tr -d '\n'`;
    return $app_id;
}


sub getAppIdFromMpstatFileName() {
    my $file_name = `ls $RESULT_HOME/*.mpstat | tail -n1 | tr -d '\n' | tr -d "'"`;

    my $app_id = `FILE_NAME=$file_name; echo \${FILE_NAME%.mpstat} | tr -d '\n'`;
    
    $app_id = `APP_ID=$app_id; echo \${APP_ID##*app}`;

    $app_id = `echo "app$app_id" | tr -d '\n'`;
    return $app_id;
}
sub getHdfsReplicationFactor {
    my ($input_path) = @_;
    my $replication_factor = `\$HADOOP_HOME/bin/hdfs dfs -stat %r $input_path/* | head -1 | perl -pe 'chomp'` ;
    $replication_factor =~ tr/\n//d;
    return $replication_factor;
}

sub getInputSize {
    my ($input_path) = @_;
    
    my $input_size = `\$HADOOP_HOME/bin/hdfs dfs -du -s $input_path | awk '{print \$1}'`;

    checkResult($input_size, "Cannot find input size!");

    $input_size =~ tr/\n//d;
    return $input_size;
}

sub getOutputSize {
    
    my ($input_path) = @_;
    my $output_size = `\$HADOOP_HOME/bin/hdfs dfs -du -s $input_path | awk '{print \$1}'`;

    checkResult($output_size, "Cannot find output size!");


    $output_size =~ tr/\n//d;
    return $output_size;
}

sub getOutputBytesFromApi {
    my ($app_id) = @_;
   
    my $scriptPath = "$FindBin::RealBin/../parse_spark_logs";
    chdir($scriptPath) or die "cannot change: $!\n";
    
    my $writeData = `python3 getOutputBytes.py $app_id`;
    $writeData =~ tr/\n//d;;
    return $writeData;
}

sub getRuntimeFromApi {
    my ($app_id) = @_;
    
    my $scriptPath = "$FindBin::RealBin/../parse_spark_logs";
    chdir($scriptPath) or die "cannot change: $!\n";
    
    my $runtime = `python3 getRuntime.py $app_id`;
    $runtime =~ tr/\n//d;;
    return $runtime;
}

sub clearCache() {
    my $scriptPath = "$FindBin::RealBin";
    chdir($scriptPath) or die "cannot change: $!\n";
#     doOnAllNodes("sudo /cm/shared/package/utils/bin/drop_caches");
    doOnAllNodes("$scriptPath/clear_buffer_cache.sh");
    #my $util_dir = "/home/omranian/spark/terasort-script";
    #doOnMasterNode("${util_dir}/free-memory.pl 0");
    #doOnMasterNode("${util_dir}/free-memory.pl 1");
}

sub killPythonSocket() {
    doOnAllNodes("pkill -f parser.py");
}

sub doOnMasterNode{
    my $command = $_[0];
    if(index($master_url, "fs3") != -1){
        system($command);
    }
    else{
        system("ssh fs3 '$command'");
    }
}

sub stopMaster(){
    system("~/scripts/spark-cluster/stop-spark.sh");
}

sub stopSpark() {
    stopMaster();
    stopWorkers();
}

sub stopWorkers() {
   doOnAllNodes("~/scripts/spark-cluster/stop-spark.sh"); 
}

sub restartHadoop() {
    system("~/restart_hadoop.sh");
}

sub doOnAllNodes{
    my $executor_node = "";
    my $command = $_[0];

    print("Running command \"${command}\" on all nodes...");
    
    # remember the directory
    my $curdir = `pwd`;
    
    # Find the common directory
    my $remoteCommandPath = "$FindBin::RealBin/..";
    chdir($remoteCommandPath) or die "cannot change: $!\n";
    print(cwd);
    
    system("./doRemoteCommand.sh '${command}'");
    
    # return back:
    chdir($curdir);
}


sub getNumaModeString {
    my ($numaMode) = @_;
    my $result = "";
    if ($numaMode == 1) {
        $result = "numa";
    }
    elsif ($numaMode == 2) {
        $result = "numaR";
    }
    elsif ($numaMode == -1) {
        $result = "default";
    }
    else {
        $result = "default";
    }

    return $result;
}

sub startSparkMaster {
    print("\nStarting Spark Master...\n");
    my $scriptPath = "$FindBin::RealBin/";
    chdir($scriptPath) or die "cannot change: $!\n";
    system("./start_spark_master.sh");
}

sub startSparkWorkers {
    my $numaMode = $_[0];
    my $numberOfExecutors = $_[1];
    my $coresPerExecutor = $_[2];
    my $memPerExecutor = $_[3];
    my $app_name = $_[4];

    my $allCommands = "";

    my $confDefault = "--cores ${coresPerExecutor} --memory ${memPerExecutor}g";


    my $numaModeString = getNumaModeString($numaMode);
    print("\n\nStarting Slaves in [${numaModeString}] mode with [${numberOfExecutors}] Executor with [${coresPerExecutor}] cores with [${memPerExecutor}] memory!!\n\n");

    foreach(@executor_nodes) {
        my $executor_node = $_;
        my $finalCmd = "";
        my $numactlCmd = "";
        my $cpuBind = 0;
        my $memBind = 0;

#         if($executorPinnedOnSocket == 1 and $numberOfExecutors == 1) {
#             $cpuBind = 1;
#             $memBind = 1;
#         }

        for (my $i=0; $i < $numberOfExecutors ; $i++) {
            if ($numaMode == 1) {
                if($i % 2 != 0) {
                    $cpuBind = 1;
                    $memBind = 1;
                }
                $numactlCmd = "numactl --cpunodebind=${cpuBind} --membind=${memBind}";
            }
            elsif ($numaMode == 2) {
                if ($i % 2 == 0) {
                    $cpuBind = 0;
                    $memBind = 1;
                }
                else {
                    $cpuBind = 1;
                    $memBind = 0;
                }
                $numactlCmd = "numactl --cpunodebind=${cpuBind} --membind=${memBind}";
            }


            my $perfCmd = "";
            my $conf = $confDefault;
            if (($executor_node eq "node302" or $executor_node eq "node304") and ($i % 2 == 0) ) {
                my $perfBaseCmd = "perf stat";
                my $outputFile = "-o /home/omranian/logs/executor/executor-${executor_node}-perf-${app_name}.csv";
                my $args = "-x, -a --per-socket";
                my $events = "-e cache-misses,cache-references";
                $events .= ",mem_load_uops_l3_miss_retired.local_dram,mem_load_uops_l3_miss_retired.remote_dram";
                #$events .= ",offcore_response.all_reads.llc_miss.local_dram,offcore_response.all_reads.llc_miss.remote_dram";
                $perfCmd = "${perfBaseCmd} ${outputFile} ${args} ${events}";
                #$conf = "--perf \"${perfCmd}\" ${conf}";
            }

            #adding strace
            #if ($start_trace_at_stage < 0 ) {
                #my $straceCmd = "strace -e trace=open,lseek,read,write -tt -T -f -o /home/omranian/logs/executor/executor-${executor_node}-strace-${app_name}.strace";
                # my $straceCmd = "strace -e trace=futex,epoll_wait,read,write  -c -f -o /home/omranian/logs/executor/executor-${executor_node}-strace-${app_name}.strace";
                #my $straceCmd = "strace -c -f -o /home/omranian/logs/executor/executor-${executor_node}-strace-${app_name}.strace";
            my $straceCmd = "strace -e trace=epoll_wait -tt -T -f -o /home/omranian/logs/executor/executor-${executor_node}-strace-${app_name}.strace";
            #$conf = "--strace \"${straceCmd}\" ${conf}";
                #}
            my $startSlaveCmd = "${base_spark_dir}/sbin/start-slave.sh $conf $master_url";
            $finalCmd = "${numactlCmd} ${startSlaveCmd}";
            $allCommands = "${allCommands} 'ssh omranian\@${executor_node} $finalCmd'";
            #print("Running command \"${finalCmd}\" on [${executor_node}]\n");
            #system("ssh omranian\@${executor_node} '${finalCmd}'");
        }

    }
    
    my $remoteCommandPath = "$FindBin::RealBin/";
    chdir($remoteCommandPath) or die "cannot change: $!\n";
     system("./remoteCommand.sh $allCommands");
    #startSparkOnNodes($numa, $startSlaveCmd, $numberOfExecutors, $app_name);
}

sub prepareCommandForAllNodes {
    my ($app_name) = @_;
    my $allCommands = "";
    foreach(@executor_nodes) {
        my $node = $_;
        my $process_finder = "ps -ef | grep \"org.apache.spark.executor.CoarseGrainedExecutorBackend\" | grep -v grep | awk \"{print \$ 2}\""; 
        #my $process_finder = "ps -ef | grep \"org.apache.hadoop.hdfs.server.datanode.DataNode\" | grep -v grep | awk \"{print \$ 2}\""; 
        my $straceCmd = "strace -c -f -o /home/omranian/logs/executor/executor-${node}-strace-${app_name}.strace -p \$($process_finder)";
        #my $straceCmd = "echo \$($process_finder)";
        $allCommands = "$allCommands 'ssh omranian\@$node $straceCmd'";
    }

    system("./remoteCommand.sh $allCommands");
}

sub startStraceOnExecutors {
    prepareCommandForAllNodes(@_);
}

1;
