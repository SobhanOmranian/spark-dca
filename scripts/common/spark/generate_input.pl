#!/usr/bin/perl

# Number of records
my $records = 300000000; # 30GB

my $partitions=16;

# Where the temporary file is going to be generated
my $workdir = "/var/scratch/omranian";

my $master_input_dir="${workdir}/input-${records}-records";
my $sort_input_dir = "${workdir}/input-${records}-records-${partitions}-parts";

sub split_input() {
  my $parts = int ($records / $partitions);
  print("sort_input_dir = ${sort_input_dir}\n");
  system("mkdir -p ${sort_input_dir}");
  system("cd ${sort_input_dir} && split -n l/$partitions  ${master_input_dir}/teraSort-input PART-");
}

sub generate_input() {
  system("mkdir -p ${master_input_dir}");
  system("./teraGenVal/gensort -a -t${par_proc} $records ${master_input_dir}/teraSort-input");
  system("echo '#INPUT SIZE: ' &&  ls -lah ${master_input_dir}/teraSort-input");
}

sub delete_input() {
  system("rm -rf ${master_input_dir}");
  system("rm -rf ${sort_input_dir}");
}

sub rm_hdfs() {
    my $HADOOP_HOME = $ENV{'HADOOP_HOME'};
    my $cmd = "$HADOOP_HOME/bin/hdfs dfs -rm -r data/input-${records}-records";
    print("\n$cmd\n");
    system($cmd)
}

sub put_hdfs() {
    my $HADOOP_HOME = $ENV{'HADOOP_HOME'};
    #my $HADOOP_MASTER = "-fs hdfs://fs3.das5.tudelft.nl:9000";
    my $HADOOP_MASTER = "";
    my $cmd = "$HADOOP_HOME/bin/hdfs dfs $HADOOP_MASTER -put ${master_input_dir}  data/";
    print("\n$cmd\n");
    system($cmd)
}

# main
delete_input();
rm_hdfs();
generate_input();
put_hdfs();
delete_input();