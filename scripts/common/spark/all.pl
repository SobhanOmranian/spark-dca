#!/usr/bin/perl

use strict;
use warnings;

use sparksetup;
use topsetup;
use mpstatSetup;
use memstallsetup;
use iostatSetup;
use dcaSetup;
use straceFileParserSetup;
use dcaStaticSetup;

#my $master_url = "spark://node348.cm.cluster:7077";
my $master_url = "spark://fs3.cm.cluster:7077";
#my $baseDir = '/home/omranian/spark-new';

my $HIBENCH_HOME = $ENV{'HIBENCH_HOME'};
if (@ARGV != 4) {
    print("You need to provide me 4 arguments!\n");
    exit;
}

my $isRun = 0;

my ($appType, $workload, $workloadSize, $runOrPrepare) = @ARGV;

if($runOrPrepare eq "run") {
    $isRun = 1;
}


sub convertWorkload {
    my $result = "";
    if ($workload eq "nweight") {
        $result = "NWeight";
    }

    elsif ($workload eq "svm") {
        $result = "SVM";
    }

    elsif ($workload eq "lda") {
        $result = "LDA";
    }
    elsif ($workload eq "als") {
        $result = "ALS";
    }
    elsif ($workload eq "bayes") {
        $result = "Bayes";
    }
    elsif ($workload eq "kmeans") {
        $result = "Kmeans";
    }
    elsif ($workload eq "join") {
        $result = "Join";
    }
    elsif ($workload eq "aggregation") {
        $result = "Aggregation";
    }
    elsif ($workload eq "scan") {
        $result = "Scan";
    }
    elsif ($workload eq "pagerank") {
        $result = "Pagerank";
    }
    elsif ($workload eq "wordcount") {
        $result = "Wordcount";
    }
    return $result;
}

my $workload_hdfs = convertWorkload();

#my $workload = 'terasort';
#my $appType = 'micro';



sub preExperimentActions() {
    clearCache();
    #stopSpark();
}



sub getAppName{
    my ($numaMode, $numExec, $cores, $mem, $dcaNum) = @_;
    my $appName = getNumaModeString($numaMode);
    my $input_size = getHiBenchInputSize();
    if($dcaNum > 0) {
        $appName = "dca${dcaNum}";
    }
    $appName .= "-${workload}";
    my $numberOfNodes = getNumberOfNodes();
    $appName .= "-${numberOfNodes}n";
    $appName .= "-${numExec}e";
    $appName .= "-${cores}c";
    $appName .= "-${mem}g";
    $appName .= "-${input_size}";

    return $appName;
}
sub runWorkload(@) {
    my $tmp_dir = "/local/omranian/spark-tmp/*";
    doOnAllNodes("rm -rf ${tmp_dir}");
    system("rm -r $ENV{'SPARK_HOME'}/work/*");
    my ($numaMode, $numExec, $cores, $mem) = @_;
    my $scriptCmd = "";
    if($isRun) {
        $scriptCmd = "$HIBENCH_HOME/bin/workloads/${appType}/${workload}/spark/run.sh";
    }
    else {
        $scriptCmd = "$HIBENCH_HOME/bin/workloads/${appType}/${workload}/prepare/prepare.sh";
    }
    #preExperimentActions();
    
    #startSpark($withNuma, $numberOfExecutors, $coresPerExecutor, $memPerExecutor);
    #return;
    system("${scriptCmd}");
    #$tmp_dir = "/var/scratch/omranian/spark-tmp/*";
    #system("rm -rf ${tmp_dir}");
    #doOnAllNodes("rm -rf ${tmp_dir}");
}




#sub runExperimentTimes {
    #my ($numaMode, $numberOfExecutors, $cores, $mem, $num) = @_;
    #for (my $i = 0; $i < $num; $i++ ) {
        #if ($i == 0) {
            #stopSpark();
            #sleep 1;
            #startSpark($numaMode, $numberOfExecutors, $cores, $mem);
        #}
        #runWorkload($numaMode, $numberOfExecutors, $cores, $mem);
    #}
#}
    #
sub rmPioArtifacts() {
        system("rm /home/omranian/results/*.pio");
}

sub savePioResult() {
        my $input_bytes = -1;
        my $output_bytes = -1;
        my $stages_output_bytes = -1;
        my $replication_factor = -1;

    
        $stages_output_bytes = getOutputBytesFromApi(getAppIdFromFileName());

        if ($isRun) {
            $input_bytes = getInputSize();
            $output_bytes = getOutputSize();
            $replication_factor = getHdfsReplicationFactor();
        }
        #print ("python3 ~/scripts/io-activity/parse-io-activity-results.py $output_bytes $replication_factor");
        system("cat ~/results/*.pio | python3 ~/scripts/io-activity/parse-io-activity-results.py $input_bytes $output_bytes $stages_output_bytes $replication_factor");
}

sub getHiBenchInputSize() {
    my $input_size = `cat $HIBENCH_HOME/conf/ihibench.conf | grep "hibench.scale.profile" | awk '{print \$2}'`;
    return $input_size;
}

sub getAppIdFromFileName() {
    my $file_name = `ls ~/results/*.pio | tail -n1 | tr -d '\n' | tr -d "'"`;
    #$file_name = `echo \${$file_name//'}`;
    
    #my $app_id = `FILE_NAME=$file_name; echo \${FILE_NAME%.pio}`;
    my $app_id = `FILE_NAME=$file_name; echo \${FILE_NAME%.pio} | tr -d '\n'`;
    #print($app_id);
    $app_id = `APP_ID=$app_id; echo \${APP_ID##*app}`;
    #print($app_id_2);
    $app_id = `echo "app$app_id" | tr -d '\n'`;
    return $app_id;
}

sub getOutputBytesFromApi {
    my  ($app_id) = @_;
    my $writeData = `python3 ~/projects/parse-spark-logs/getOutputBytes.py $app_id`;
    #print("\n\noutput bytes = $writeData\n\n")
    $writeData =~ tr/\n//d;;
    return $writeData;
}

sub getInputSize {
    my $input_size = `\$HADOOP_HOME/bin/hdfs dfs -du -s HiBench/$workload_hdfs/Input | awk '{print \$1}'`;

    checkResult($input_size, "Cannot find output size!");


    $input_size =~ tr/\n//d;
    return $input_size;
}

sub checkResult {
    my ($var, $msg) = @_;
    #if (index($var, "No such file") != -1) {
    #print("[Error] $msg");
    #exit();
    #}

    
    if ($var eq "") {
        print("[Error] $msg\n");
        exit();
    }
}

sub getOutputSize {
    my $input_size = `\$HADOOP_HOME/bin/hdfs dfs -du -s HiBench/$workload_hdfs/Output | awk '{print \$1}'`;
    
    if ($input_size eq "") {
        $input_size = 0;
    }

    checkResult($input_size, "Cannot find output size!");

    $input_size =~ tr/\n//d;
    return $input_size;
}

sub getHdfsReplicationFactor {
    my $replication_factor = `\$HADOOP_HOME/bin/hdfs dfs -stat %r HiBench/$workload_hdfs/Input/* | head -1 | perl -pe 'chomp'` ;
    $replication_factor =~ tr/\n//d;
    return $replication_factor;
}

sub setWorkloadSize {
    system("sed -i 's/^hibench.scale.profile .*\$/hibench.scale.profile $workloadSize/' $HIBENCH_HOME/conf/hibench.conf");
}

sub setAdaptiveThreadpool {
    my ($adaptive) = @_;
        system("sed -i 's/^spark.executorEnv.SPARK_ADAPTIVE_THREADPOOL .*\$/spark.executorEnv.SPARK_ADAPTIVE_THREADPOOL $adaptive/' $HIBENCH_HOME/conf/spark.conf");
}


    
sub runExperimentTimes {
    my ($numaMode, $numberOfExecutors, $cores, $mem, $num, $dcaMode, $dcaNum, $adaptiveThreadPool) = @_;
    $ENV{'SPARK_DCA'} = $dcaMode;
    $ENV{'SPARK_DCA_PARALLELISM'} = $dcaNum;
    $ENV{'SPARK_ADAPTIVE_THREADPOOL'} = $adaptiveThreadPool;
    $ENV{'SPARK_DCA_UPDATE_SCHEDULER'} = 1;
    setAdaptiveThreadpool($adaptiveThreadPool);
    setDcaStatic($dcaMode);
    setDcaStaticThreadNum($dcaNum);
    for (my $i = 0; $i < $num; $i++ ) {
        my $appName = getAppName($numaMode, $numberOfExecutors, $cores, $mem, $dcaNum);
        $ENV{'SPARK_APP_NAME'} = $appName;
        $ENV{'SPARK_EXEC_MEM'} = $mem;
        #print($appName);
        #last;

        preExperimentActions();
        #restartHadoop();
        killPythonSocket();
        killMpstatSocket();
        killIostatSocket();
        killTopSocket();
        killMemstallSocket();
        killStraceFileParserSocket();
        # delete previous pio results
        rmPioArtifacts();
        rmMpstatArtifacts();
        rmIostatArtifacts();
        rmTopArtifacts();
        rmMemstallArtifacts();
        rmDcaArtifacts();
        rmStraceFileParserArtifacts();

        startSparkWorkers($numaMode, $numberOfExecutors, $cores, $mem, $appName);
        setWorkloadSize();
        print("\nWaiting for 3 seconds before starting...\n");
        sleep(3);
        runWorkload($numaMode,$numberOfExecutors,$cores,$mem);
        stopWorkers();
        killPythonSocket();
        killMpstat();
        killMpstatSocket();
        killIostat();
        killIostatSocket();
        killTop();
        killTopSocket();
        killMemstall();
        killMemstallSocket();
        killStraceFileParser();
        killStraceFileParserSocket();
        print("\nWaiting for 7 second for pio to finish writing...\n");
        sleep(7);
        savePioResult();
        saveMpstatResult();
        saveTopResult();
        saveMemstallResult();
        saveIostatResult();
        system("cp /home/omranian/results/*.iostat /home/omranian/results/trunk/.");
        saveDcaResult(getFullAppNameFromFileName(), $adaptiveThreadPool, $cores * (getNodeCount() * $numberOfExecutors));
        saveStraceFileParserResult();
    }
}


#runWorkload(2, 2, 16, 16);
#runWorkload(1);
#runWorkload(2);
#

#my $appId = getAppIdFromFileName();
#print($appId);
#getAppWriteDataFromApi($appId);
#my $factor = getHdfsReplicationFactor();
#print("\n$factor");
#savePioResult();
#exit();
#print(getInputSize());
#exit();

stopSpark();
startSparkMaster();
my $numberOfExperiments = 1;

if ($isRun) {
    $numberOfExperiments = 1;
}
my $mem = 56;
#runExperimentTimes(-1, 1, 32, 48, $numberOfExperiments);
#runExperimentTimes(0, 1, 32, $mem, $numberOfExperiments, 0, 0, 100);


runExperimentTimes(0, 1, 32, $mem, $numberOfExperiments, 0, 0, 100);
if ($isRun) {
    #runExperimentTimes(0, 1, 16, $mem, $numberOfExperiments, 0, 0, 100);
    #runExperimentTimes(0, 1, 8, $mem, $numberOfExperiments, 0, 0, 100);
    #runExperimentTimes(0, 1, 4, $mem, $numberOfExperiments, 0, 0, 100);
    #runExperimentTimes(0, 1, 2, $mem, $numberOfExperiments, 0, 0, 100);
}
#runExperimentTimes(0, 1, 8, $mem, $numberOfExperiments, 0, 0, 100);
#runExperimentTimes(0, 1, 32, $mem, $numberOfExperiments, 0, 0, 13);
#runExperimentTimes(0, 1, 8, $mem, $numberOfExperiments, 0, 0, 14);
#runExperimentTimes(0, 1, 32, $mem, $numberOfExperiments, 0, 0, 99);
#runExperimentTimes(0, 1, 16, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 8, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 4, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 4, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 32, $mem, $numberOfExperiments, 1, 8);
#runExperimentTimes(0, 1, 16, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 8, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 4, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 2, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 1, 1, $mem, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 2, 1, 10, $numberOfExperiments, 0, 0);
#runExperimentTimes(0, 2, 16, 24, $numberOfExperiments);
#runExperimentTimes(0, 2, 8, 24, $numberOfExperiments);
#runExperimentTimes(0, 2, 4, 24, $numberOfExperiments);
#runExperimentTimes(0, 2, 1, 24, $numberOfExperiments);
#runExperimentTimes(1, 2, 16, 24, $numberOfExperiments);
#runExperimentTimes(2, 2, 16, 24, $numberOfExperiments);
