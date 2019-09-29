# Figure 2 & 4
python3 figure2_figure4_experiment[static_solution]/plot_dca_static.py -i $RESULT_HOME/resultDcaStatic.csv  -o $RESULT_HOME/pdf &
# Figure 3
#python3 figure3_experiment\[io_variability\]/plot_io_benchmark.py -i /Users/sobhan/phd/data/dca/data_files/figure3_experiment\[io_variability\].csv -o /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big\ Data\ Paradigm/img/das_io_experiment.pdf &
# Figure 5
python3 figure5_experiment\[io_utilization\]/plot_io_util.py -i $RESULT_HOME/resultIostat.csv -o $RESULT_HOME/pdf &
# Figure 6
python3 figure6_experiment\[thread_selection\]/plot_dca_thread_selection.py -i $RESULT_HOME/combined.dca -o $RESULT_HOME/pdf/dca_core_plot.pdf &
# Figure 7
python3 figure7_experiment[epoll_wait_and_throughput]/plot_epoll_throughput.py -i $RESULT_HOME/combined.dca  -o $RESULT_HOME/pdf &
# Figure 8
python3 figure8_experiment[dynamic_solution]/plot_dca_dynamic.py -i $RESULT_HOME/resultDca.csv -o $RESULT_HOME/pdf -d hdd &
# Figure 9
#python3 figure9_experiment[scalability]/plot_dca_scalability.py  -i /Users/sobhan/phd/data/dca/data_files/figure9_experiment\[scalability\].csv -o /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big\ Data\ Paradigm/img/dca_dynamic_scalability.pdf &
# Figure 10
python3 figure10_experiment\[static_solution_ssd\]/plot_dca_static_ssd.py -i $RESULT_HOME/resultDcaStatic.csv -o $RESULT_HOME/pdf &
# Figure 11
python3 figure8_experiment[dynamic_solution]/plot_dca_dynamic.py -i $RESULT_HOME/resultDca.csv -o $RESULT_HOME/pdf -d ssd &
# Figure 12
python3 figure12_experiment[io_performance]/plot_io_performance.py -i $RESULT_HOME/iostat.combined  -o $RESULT_HOME/pdf -d hdd &
#python3 figure12_experiment[io_performance]/plot_io_performance.py -i /Users/sobhan/phd/data/dca/data_files/figure12_experiment\[io_performance_sdd\].csv  -o /Users/sobhan/scala-ide-workspace-spark/spark/publication/Big\ Data\ Paradigm/img -d ssd &
wait
