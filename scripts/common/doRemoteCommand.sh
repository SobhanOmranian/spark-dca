#!/bin/bash

function printBorder {
    howMany=$1
    #echo $howMany
    for i in `seq 1 $howMany`; do
        printf '='
    done
    echo -e '\n'
}

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

SLAVE_NAMES=$(cat "slaves" | sed  's/#.*$//;/^$/d')
#for slave in $SLAVE_NAMES; do
    #echo $slave
#done

cmd=$1
sshCmd='ssh '
len=$((${#cmd} + ${#sshCmd}))
#echo $len

echo -e '\n'
printBorder $len
for slave in $SLAVE_NAMES; do
    echo "$sshCmd$slave '$cmd'"
    eval "$sshCmd$slave '$cmd'" &
done
wait
printBorder $len
echo -e '\n'

