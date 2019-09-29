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

if (@ARGV < 4) {
        print("You need to provide me at least 4 arguments for (1) mode (2) number of times (3) isStatic (4) max number of threads!\n");
        exit;
}

my ($mode, $numberOfTimes, $isStatic, $maxNumberOfThreads) = @ARGV;
my $ioNumberOfThreads = 0;
my $shouldRevert = 0;

if ($mode == 14 && !$isStatic) {
    # static mode
    if (@ARGV < 5) {
        print("You need to provide me at least 5 arguments for dynamic mode! (1) mode = 100 (2) number of times (3) isStatic = 1 (4) max number of threads! (5) should tuning explore the whole space.\n");
        exit;
    }
     ($mode, $numberOfTimes, $isStatic, $maxNumberOfThreads, $shouldRevert) = @ARGV;
}

if ($mode == 100 && $isStatic) {
    # static mode
    if (@ARGV < 5) {
        print("You need to provide me at least 5 arguments for static mode! (1) mode = 100 (2) number of times (3) isStatic = 1 (4) max number of threads! max number of threads for non-io stages (5) number of threads for io-stages.\n");
        exit;
    }
     ($mode, $numberOfTimes, $isStatic, $maxNumberOfThreads, $ioNumberOfThreads) = @ARGV;
}



# Spark Master Url
my $master_url = "spark://fs3.cm.cluster:7077";

# Number of records
my $records = 150000000; # 15GB

# Where spark is going to store temporary files
my $tempWorkdir = $ENV{'SPARK_WORK_DIR'};
# Where the input and output data files are
my $sort_input_dir = "hdfs://fs3.das5.tudelft.nl:9000//user/omranian/data/input-${records}-records";
my $sort_output_dir = "hdfs://fs3.das5.tudelft.nl:9000//user/omranian/data/output-${records}-records";

# Interrupt the execution at the given stage. -1 for not stopping.
my $stop_at_stage = -1;

# How many times each experiment should be run
my $numberOfExperiments = $numberOfTimes;

my $updateScheduler = 0;
if ($mode == 14 || $isStatic == 1) {
    $updateScheduler = 1;
}

my @values;
my $class_name = "Main";
my $RESULT_HOME = $ENV{'RESULT_HOME'};
my $SCRIPT_HOME = "$ENV{'SPARK_HOME'}/scripts";
my $SPARK_HOME = $ENV{'SPARK_HOME'};



sub getAppName {
    my $app_name = "";

    my ($numaMode, $numberOfExecutors, $cores, $mem, $runId, $adaptiveThreadPool, $updateScheduler, $strace) = @_;

    my $experiment_name = "terasort-hdfs";

    my $numaModeString = getNumaModeString($numaMode);
    $app_name .= "${numaModeString}";
    if($isStatic) {
        # static mode
        $app_name .= "-static$ioNumberOfThreads";
    }
    $app_name .= "-${experiment_name}";
    $app_name .= "-${class_name}";
    $app_name .= "-${records}r";
    $app_name .= "-${numberOfExecutors}e";
    
    $app_name .= "-${cores}c";
    $app_name .= "-${mem}G";

    my $numberOfNodes = getNumberOfNodes();
    $app_name .= "-${numberOfNodes}n";

    $app_name .= "-${adaptiveThreadPool}a";
    $app_name .= "-${updateScheduler}us";
    $app_name .= "-${strace}st";

    return $app_name;
}

sub run_job(@){
    my ($withNuma) = $_[0];
    my $execNum = $_[1];
    my $num_cores = $_[2];
    my $memory = $_[3];
    my $runId = $_[4];
    my $app_name = $_[5];
    my $adaptiveThreadPool = $_[6];
    my $updateScheduler = $_[7];
    my $strace = $_[8];

    my $numberOfNodes = getNumberOfNodes();

    my $value;

    #i*STDERR = *STDOUT;
    my $conf = "";
    my $adaptiveThreadConf = "--conf spark.executorEnv.SPARK_ADAPTIVE_THREADPOOL=$adaptiveThreadPool";
    $conf = "${conf} $adaptiveThreadConf";
    my $updateSchedulerConf = "--conf spark.executorEnv.SPARK_DCA_UPDATE_SCHEDULER=$updateScheduler";
    $conf = "${conf} $updateSchedulerConf";
    my $straceConf = "--conf spark.executorEnv.SPARK_DCA_STRACE=$strace";
    $conf = "${conf} $straceConf";

    if ($isStatic) {
        # static mode
        my $dcaStaticConf = "--conf spark.executorEnv.SPARK_DCA_STATIC=1";
        $conf = "${conf} $dcaStaticConf";
    
        my $dcaStaticNumConf = "--conf spark.executorEnv.SPARK_DCA_STATIC_THREAD_NUM=$ioNumberOfThreads";
        $conf = "${conf} $dcaStaticNumConf";
    }

    my $myPath = "$FindBin::RealBin";
    my $perl_command =  "$SPARK_HOME/bin/spark-submit $conf --conf \"spark.hadoop.dfs.replication=1\"  --conf \"spark.executor.memory=${memory}g\"  --conf \"spark.local.dir=${tempWorkdir}/spark-tmp\"  --class ${class_name} --driver-library-path `pwd`/../../libs:/opt/ibm/capikv/lib --driver-java-options \"-Djava.io.tmpdir=${tempWorkdir}/spark-tmp\" --master ${master_url} $myPath/target/terasort.jar ${app_name} \'${sort_input_dir}/\*\'  ${sort_output_dir} 0 -1 -1 2>&1 |";
    print("perl command = ${perl_command}\n");
    my $pid = open my $pipe, $perl_command or die("failed to open");
    print ("\npid = $pid\n");
    while (my $line = <$pipe>) {
        chomp ($line);
        print("$line\n");
        if ($line =~ /^Elapsed time: (\d+)$/) {
            $value = $1;
        }

        
        elsif ($line =~ m/\d \([\s\S]+\) finished in (\d*\.?\d*) s/g) {
            my $time = $1;
            my $stageNum = 999;
            if ($line =~ m/(\d*) \([\s\S]+\) finished/) {
                print("\nwhile match!\n");
                print("$1\n");
                $stageNum = $1;
            }


            if ($stageNum == $stop_at_stage) {
                print("\nWe have reached stage $stop_at_stage, stoppin...\n");
 
                system("kill -2 \$(jps | grep SparkSubmit | awk '{print \$1}')");
                #system("kill -INT $pid");
            }
            if ($stageNum <= $stop_at_stage) {
                $value += $time;
            }
        }
    }


    return $value;
}

sub evaluate_values (@) {
  my $n = @_;
  my $avg = sum(@_)/$n;
  my $min = min(@_);
  my $max = max(@_);
  my $std_dev = ($min == $max) ? 0 : sqrt(sum(map {($_ - $avg) ** 2} @_) / $n);
  return ($avg, $std_dev, $min, $max);
}

sub validate() {
  system("./scripts/teraval.py ${sort_output_dir} part- -threads 24 1>&2");
}

sub delete_output() {
  doOnAllNodes("rm -rf ${tempWorkdir}/spark-tmp/*");
  doOnMasterNode("$ENV{'HADOOP_HOME'}/bin/hdfs dfs -rm -r data/output-${records}-records");
}

sub delete_splits() {
  system("rm -rf ${sort_input_dir}");
}

sub preExperimentActions() {
    clearCache();
    delete_output();
}


###
# main
###

print STDERR "Starting benchmark";
print "# teraSort\n";
print "# $records records\n";
my $runtime;

sub savePioResult() {
        my $input_bytes = getInputSize("data/input-${records}-records");
        my $output_bytes = getOutputSize("data/output-${records}-records");
        my $stages_output_bytes = getOutputBytesFromApi(getAppIdFromFileName());
        my $replication_factor = getHdfsReplicationFactor("data/input-{$records}-records");
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



sub runExperimentTimes {
    my ($numaMode, $numberOfExecutors, $cores, $mem, $num, $adaptiveThreadPool, $updateScheduler, $strace) = @_;
    if ($isStatic) {
        # static mode
        $ENV{'SPARK_DCA_STATIC'} = 1;
        $ENV{'SPARK_DCA_STATIC_THREAD_NUM'} = $ioNumberOfThreads;
    }
    $ENV{'SPARK_DCA_UPDATE_SCHEDULER'} = $updateScheduler;
    $ENV{'SPARK_DCA_SHOULD_REVERT'} = $shouldRevert;
    for (my $i = 0; $i < $num; $i++ ) {
        my $appName = getAppName($numaMode, $numberOfExecutors, $cores, $mem, $num, $adaptiveThreadPool, $updateScheduler, $strace);
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
        my $runtime  = run_job($numaMode, $numberOfExecutors, $cores, $mem, $i, $appName, $adaptiveThreadPool, $updateScheduler, $strace);
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
        saveIostatResult(getFullAppNameFromFileName());
        sleep(3);
        saveDcaResult(getFullAppNameFromFileName(), $adaptiveThreadPool, $cores * ($numberOfNodes * $numberOfExecutors));
        # static mode and default
        if ($isStatic || $mode == 100) {
            saveDcaStaticResult(getFullAppNameFromFileName(), $adaptiveThreadPool, $cores * ($numberOfNodes * $numberOfExecutors));
        }
        delete_output();
        push(@values, $runtime);
        print("\n\n================\n\n");
        print("Run time: ${runtime} \n\n");
        print "\n\n\n\n";
    }
}

stopSpark();
startSparkMaster();
setup($tempWorkdir);

runExperimentTimes(0, 1, $maxNumberOfThreads, 56, $numberOfExperiments, $mode, $updateScheduler, 0);
report(@values); 
