my $RESULT_HOME = $ENV{'RESULT_HOME'};
my $SCRIPT_HOME = "$ENV{'SPARK_HOME'}/scripts";
    
sub rmDcaArtifacts {
    system("rm $RESULT_HOME/*.dca");
    system("rm $RESULT_HOME/*.taskfinish");
}

sub saveDcaResult {
    system("$SCRIPT_HOME/common/combine_dca.sh");

    # appName, stage, adaptive, usedCores, totalCores
    my ($appName, $adaptive, $totalCores) = @_;
    my $topFilePath = "$RESULT_HOME/resultTop.csv";
    system("python3 $SCRIPT_HOME/figure8_experiment[dynamic_solution]/parse_dca_result.py --appName $appName --adaptive $adaptive --total_cores $totalCores --topFile $topFilePath --outputFile $RESULT_HOME/resultDca.csv");
}

sub saveDcaStaticResult {
#     system("$SCRIPT_HOME/common/combine_dca.sh");
    
    my ($appName, $adaptive, $totalCores) = @_;
    my $topFilePath = "$RESULT_HOME/resultTop.csv";
    system("python3 $SCRIPT_HOME/figure2_figure4_experiment[static_solution]/parse_dca_static_result.py --appName $appName --adaptive $adaptive --total_cores $totalCores --topFile $topFilePath --outputFile $RESULT_HOME/resultDcaStatic.csv");
}

1;
