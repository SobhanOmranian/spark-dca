sub setIoStageList {
    my ($ioList) = @_;
    system("sed -i 's/^spark.executorEnv.SPARK_DCA_IO_STAGE_LIST .*\$/spark.executorEnv.SPARK_DCA_IO_STAGE_LIST $ioList/' conf/spark.conf");
}

sub setIoStageThreadNum {
    my ($threadNum) = @_;
    system("sed -i 's/^spark.executorEnv.SPARK_DCA_IO_STAGE_THREAD_NUM .*\$/spark.executorEnv.SPARK_DCA_IO_STAGE_THEADNUM $threadNum/' conf/spark.conf");
}
1;
