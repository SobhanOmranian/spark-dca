#!/usr/bin/perl

use strict;
use warnings;



#my $records = 300000000;
#my $records = 200000000;
#my $records = 100000000;
#my $records = 90000000;
#my $records = 75000000;
#my $records = 50000000;
#my $records = 20000000; # 200 vs 195 seconds
my $records = 10000000; # 115 vs 108 seconds
#my $records = 200000;

my $partitions = 8;
my $par_proc = 80;

my $workdir = "/var/scratch/omranian/tmp";
my $master_input_dir="${workdir}/input-${records}-records";
my $sort_input_dir = "${workdir}/input-${records}-${partitions}-records";


sub split_input() {
  my $parts = int ($records / $partitions);
  print("sort_input_dir = ${sort_input_dir}\n");
  system("mkdir -p ${sort_input_dir}");
  system("cd ${sort_input_dir} && split -n l/$partitions  ${master_input_dir}/teraSort-input PART-");
}

sub generate_input() {
  system("mkdir -p ${master_input_dir}");
  system("./teraGenVal/gensort -a -t${par_proc} $records ${master_input_dir}/teraSort-input");
  #my $sort_input_dir = "./storage"
  system("echo '#INPUT SIZE: ' &&  ls -lah ${master_input_dir}/teraSort-input");
  print "\nPutting the data in HDFS...";
  system("~/links/hadoop/bin/hdfs dfs -put -f ${master_input_dir} data/");
  print "\nDeleting the input from local disk...";
  delete_input();
}


sub delete_input() {
  system("rm -rf ${master_input_dir}");
}

###
# main
###

print STDERR "Starting Terasort Data Generator with ${records} records...";
generate_input();
print "\n";
