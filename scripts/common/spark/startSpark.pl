#!/usr/bin/perl

use strict;
use warnings;

use sparkSetup;

stopSpark();
startSparkMaster();
startSparkWorkers(0, 1, 32, 48, "nothing");
