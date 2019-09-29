#!/usr/bin/perl -Ilib
use strict;
use warnings;
use List::Util qw(sum min max);
use Cwd;

use FindBin;
use lib "$FindBin::RealBin/lib";

use sparkSetup;
use topSetup;
use memstallSetup;
use mpstatSetup;
use iostatSetup;
use dcaSetup;
use firstTimeSetup;
use ioSchedulerSetup;
use commonSetup;

if (@ARGV < 8) {
        print("You need to provide me at least 8 arguments for (1) workload type (2) workload name (3) workload size (4) run or prepare (5) mode (6) number of times (7) isStatic (8) max number of threads\n");
        exit;
}

my ($appType, $workload, $workloadSize, $runOrPrepare, $mode, $numberOfTimes, $isStatic, $maxNumberOfThreads) = @ARGV;
my $ioNumberOfThreads = 0;
my $shouldRevert = 1;

if ($mode == 14 && !$isStatic) {
    # static mode
    if (@ARGV < 9) {
        print("You need to provide me at least 9 arguments for dynamic mode! (1) workload type (2) workload name (3) workload size (4) run or prepare (5) mode = 100 (6) number of times (7) isStatic = 1 (8) max number of threads! (9) should tuning stop if it gets worse.\n");
        exit;
    }
     ($appType, $workload, $workloadSize, $runOrPrepare, $mode, $numberOfTimes, $isStatic, $maxNumberOfThreads, $shouldRevert) = @ARGV;
}

if ($mode == 100 && $isStatic) {
    # static mode
    if (@ARGV < 9) {
        print("You need to provide me at least 9 arguments for static mode! (1) workload type (2) workload name (3) workload size (4) run or prepare (5) mode = 100 (6) number of times (7) isStatic = 1 (8) max number of threads for non-io stages (9) number of threads for io-stages.\n");
        exit;
    }
     ($appType, $workload, $workloadSize, $runOrPrepare, $mode, $numberOfTimes, $isStatic, $maxNumberOfThreads, $ioNumberOfThreads) = @ARGV;
}

my $isRun = 0;
if($runOrPrepare eq "run") {
    $isRun = 1;
}

# Spark Master Url
my $master_url = "spark://fs3.cm.cluster:7077";

# Number of records
my $records = 150000000; # 15GB

# Where spark is going to store temporary files
my $tempWorkdir = $ENV{'SPARK_WORK_DIR'};

# Interrupt the execution at the given stage. -1 for not stopping.
my $stop_at_stage = -1;

# How many times each experiment should be run
my $numberOfExperiments = $numberOfTimes;

my $updateScheduler = 0;
if ($mode == 14 || $isStatic == 1) {
    $updateScheduler = 1;
}

my @values;
my $RESULT_HOME = $ENV{'RESULT_HOME'};
my $SCRIPT_HOME = "$ENV{'SPARK_HOME'}/scripts";
my $SPARK_HOME = $ENV{'SPARK_HOME'};
my $HIBENCH_HOME = $ENV{'HIBENCH_HOME'};
my $workload_hdfs = convertWorkload();


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


sub getAppName{
    my ($numaMode, $numExec, $cores, $mem) = @_;
    my $appName = getNumaModeString($numaMode);
    
    if($isStatic) {
        # static mode
        $appName .= "-static$ioNumberOfThreads";
    }
    
    my $input_size = getHiBenchInputSize();

    $appName .= "-${workload}";
    my $numberOfNodes = getNumberOfNodes();
    $appName .= "-${numberOfNodes}n";
    $appName .= "-${mode}a";
    $appName .= "-${numExec}e";
    $appName .= "-${cores}c";
    $appName .= "-${mem}g";
    $appName .= "-${updateScheduler}us";
    $appName .= "-${input_size}";

    return $appName;
}

sub runWorkload {
    my $scriptCmd = "";
    if($isRun) {
        $scriptCmd = "$HIBENCH_HOME/bin/workloads/${appType}/${workload}/spark/run.sh";
    }
    else {
        $scriptCmd = "$HIBENCH_HOME/bin/workloads/${appType}/${workload}/prepare/prepare.sh";
    }
    system("${scriptCmd}");
}




sub preExperimentActions() {
    doOnAllNodes("rm -rf ${tempWorkdir}/spark-tmp/*");
    clearCache();
}


###
# main
###

print STDERR "Starting benchmark";

sub savePioResult() {
        my $input_bytes = getInputSize("HiBench/$workload_hdfs/Input");
        my $output_bytes = getOutputSize("HiBench/$workload_hdfs/Output");
        my $stages_output_bytes = getOutputBytesFromApi(getAppIdFromFileName());
        my $replication_factor = getHdfsReplicationFactor("HiBench/$workload_hdfs/Input/*");
        my $output_file = "$RESULT_HOME/resultPio.csv";
        system("cat $RESULT_HOME/*.pio | python3 $SCRIPT_HOME/table2_experiment[io_activity]/parse-io-activity-results.py $input_bytes $output_bytes $stages_output_bytes $replication_factor $output_file");
}

sub rmPioArtifacts() {
    system("rm $RESULT_HOME/*.pio");
}

sub checkResult {
    my ($var, $msg) = @_;
    if ($var eq "") {
        print("[Error] $msg\n");
        exit();
    }
}

sub getHiBenchInputSize() {
    my $input_size = `cat $HIBENCH_HOME/conf/hibench.conf | grep "hibench.scale.profile" | awk '{print \$2}'`;
    return $input_size;
}

sub setWorkloadSize {
    system("sed -i 's/^hibench.scale.profile .*\$/hibench.scale.profile $workloadSize/' $HIBENCH_HOME/conf/hibench.conf");
}

sub setExecutorEnvironmentVars {
    my ($adaptive, $updateScheduler, $strace) = @_;
    system("sed -i 's/^spark.executorEnv.SPARK_ADAPTIVE_THREADPOOL .*\$/spark.executorEnv.SPARK_ADAPTIVE_THREADPOOL $adaptive/' $HIBENCH_HOME/conf/spark.conf");
    system("sed -i 's/^spark.executorEnv.SPARK_DCA_UPDATE_SCHEDULER .*\$/spark.executorEnv.SPARK_DCA_UPDATE_SCHEDULER $updateScheduler/' $HIBENCH_HOME/conf/spark.conf");
    system("sed -i 's/^spark.executorEnv.SPARK_DCA_STRACE .*\$/spark.executorEnv.SPARK_DCA_STRACE $strace/' $HIBENCH_HOME/conf/spark.conf");
    
    system("sed -i 's/^spark.executorEnv.SPARK_DCA_STATIC .*\$/spark.executorEnv.SPARK_DCA_STATIC $isStatic/' $HIBENCH_HOME/conf/spark.conf");
    system("sed -i 's/^spark.executorEnv.SPARK_DCA_STATIC_THREAD_NUM .*\$/spark.executorEnv.SPARK_DCA_STATIC_THREAD_NUM $ioNumberOfThreads/' $HIBENCH_HOME/conf/spark.conf");
    system("sed -i 's/^spark.executorEnv.SPARK_DCA_SHOULD_REVERT .*\$/spark.executorEnv.SPARK_DCA_SHOULD_REVERT $shouldRevert/' $HIBENCH_HOME/conf/spark.conf");
    
    if ($isStatic) {
        # static mode
        $ENV{'SPARK_DCA_STATIC'} = 1;
        $ENV{'SPARK_DCA_STATIC_THREAD_NUM'} = $ioNumberOfThreads;
    }

    $ENV{'SPARK_DCA_UPDATE_SCHEDULER'} = $updateScheduler;
    $ENV{'SPARK_DCA_SHOULD_REVERT'} = $shouldRevert;
}

sub runExperimentTimes {
    my ($numaMode, $numberOfExecutors, $cores, $mem, $num, $adaptiveThreadPool, $updateScheduler) = @_;
    setExecutorEnvironmentVars($adaptiveThreadPool, $updateScheduler, 0);
    for (my $i = 0; $i < $num; $i++ ) {
        my $appName = getAppName($numaMode, $numberOfExecutors, $cores, $mem);
        $ENV{'SPARK_APP_NAME'} = $appName;
        $ENV{'SPARK_EXEC_MEM'} = $mem;
        preExperimentActions();
        killPythonSocket();
        killMpstatSocket();
        killIostatSocket();
        killTopSocket();
        killMemstallSocket();
        # delete previous pio results
        rmPioArtifacts();
        rmMpstatArtifacts();
        rmIostatArtifacts();
        rmTopArtifacts();
        rmMemstallArtifacts();
        rmDcaArtifacts();
        startSparkWorkers($numaMode, $numberOfExecutors, $cores, $mem, $appName);
        
        setWorkloadSize();
        print("\nWaiting for 3 seconds before starting...\n");
        sleep(3);
        runWorkload($numaMode,$numberOfExecutors,$cores,$mem);
#         stopWorkers();
        killPythonSocket();
        killMpstat();
        killMpstatSocket();
        killIostat();
        killIostatSocket();
        killTop();
        killTopSocket();
        killMemstall();
        killMemstallSocket();
        doOnAllNodes("killall strace");
        print("\nWaiting for 7 second for scripts to finish writing...\n");
        sleep(7);
        my $numberOfNodes = getNumberOfNodes();
        # all modes
        saveTopResult();
        savePioResult();
        saveMpstatResult();
        #saveMemstallResult();
        saveIostatResult();
        sleep(3);
        saveDcaResult(getFullAppNameFromFileName(), $adaptiveThreadPool, $cores * ($numberOfNodes * $numberOfExecutors));
        # static mode and default
        if ($isStatic || $mode == 100) {
            saveDcaStaticResult(getFullAppNameFromFileName(), $adaptiveThreadPool, $cores * ($numberOfNodes * $numberOfExecutors));
        }
    }
}

stopSpark();
startSparkMaster();
setup($tempWorkdir);

runExperimentTimes(0, 1, $maxNumberOfThreads, 56, $numberOfExperiments, $mode, $updateScheduler);