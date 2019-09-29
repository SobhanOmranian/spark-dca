#!/bin/bash
kill -9 $(jps -l | grep spark | awk -F ' ' '{print $1}')

