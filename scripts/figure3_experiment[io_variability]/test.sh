IFS=$'\n' read -d '' -r -a nodes < slaves
num_nodes=$(wc -l < "$1")

for node in ${nodes[@]}
do
    echo $node
done